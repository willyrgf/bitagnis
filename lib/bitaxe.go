package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	dhcpStart          = 1
	dhcpEnd            = 254
	defaultHTTPTimeout = 5 * time.Second
	scanWorkerLimit    = 64
	maxAPIResponseSize = 1 << 20
	maxAPIRequestSize  = 10_240
)

type Info struct {
	Version                string   `json:"version"`
	ASICModel              string   `json:"ASICModel"`
	BoardVersion           string   `json:"boardVersion"`
	CoreVoltage            int      `json:"coreVoltage"`
	CoreVoltageActual      float64  `json:"coreVoltageActual"`
	ErrorPercentage        *float64 `json:"errorPercentage"`
	ExpectedHashRate       float64  `json:"expectedHashrate"`
	FanRPM                 int      `json:"fanrpm"`
	FanSpeed               float64  `json:"fanspeed"`
	Frequency              int      `json:"frequency"`
	HashRate               float64  `json:"hashRate"`
	Hostname               string   `json:"hostname"`
	MacAddr                string   `json:"macAddr"`
	OverHeatMode           int      `json:"overheat_mode"`
	Power                  float64  `json:"power"`
	PowerFault             *string  `json:"power_fault"`
	SharesAccepted         uint64   `json:"sharesAccepted"`
	SharesRejected         uint64   `json:"sharesRejected"`
	StratumURL             string   `json:"stratumURL"`
	StratumPort            int      `json:"stratumPort"`
	StratumUser            string   `json:"stratumUser"`
	FallbackStratumURL     string   `json:"fallbackStratumURL"`
	FallbackStratumPort    int      `json:"fallbackStratumPort"`
	FallbackStratumUser    string   `json:"fallbackStratumUser"`
	IsUsingFallbackStratum int      `json:"isUsingFallbackStratum"`
	Temp                   float64  `json:"temp"`
	UpTimeSeconds          int      `json:"uptimeSeconds"`
	VRTemp                 float64  `json:"vrTemp"`
}

type ASICSettings struct {
	ASICModel        string `json:"ASICModel"`
	DefaultFrequency int    `json:"defaultFrequency"`
	DefaultVoltage   int    `json:"defaultVoltage"`
	FrequencyOptions []int  `json:"frequencyOptions"`
	VoltageOptions   []int  `json:"voltageOptions"`
}

type OperatingPoint struct {
	Frequency   int
	CoreVoltage int
}

type BitaxeClient struct {
	httpClient *http.Client
}

func NewBitaxeClient(timeout time.Duration) *BitaxeClient {
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}

	var transport *http.Transport
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = defaultTransport.Clone()
	} else {
		transport = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 2
	transport.MaxConnsPerHost = 4
	transport.IdleConnTimeout = 90 * time.Second
	transport.ResponseHeaderTimeout = timeout
	transport.DisableCompression = true

	return &BitaxeClient{
		httpClient: &http.Client{
			Timeout:       timeout,
			Transport:     transport,
			CheckRedirect: rejectRedirect,
		},
	}
}

func newBitaxeClient(httpClient *http.Client) *BitaxeClient {
	if httpClient == nil {
		return &BitaxeClient{}
	}
	cloned := *httpClient
	cloned.CheckRedirect = rejectRedirect
	return &BitaxeClient{httpClient: &cloned}
}

func rejectRedirect(_ *http.Request, _ []*http.Request) error {
	return fmt.Errorf("Bitaxe redirects are not allowed")
}

func (client *BitaxeClient) GetSystemInfo(ctx context.Context, target string) (Info, error) {
	var info Info
	if err := client.getJSON(ctx, target, "/api/system/info", &info); err != nil {
		return Info{}, fmt.Errorf("get system info: %w", err)
	}
	if info.ErrorPercentage != nil &&
		(!finite(*info.ErrorPercentage) ||
			*info.ErrorPercentage < 0 ||
			*info.ErrorPercentage > 100) {
		info.ErrorPercentage = nil
	}
	if err := validateInfo(info); err != nil {
		return Info{}, fmt.Errorf("validate system info: %w", err)
	}
	info.MacAddr, _ = normalizeMAC(info.MacAddr)
	return info, nil
}

func (client *BitaxeClient) GetASICSettings(
	ctx context.Context,
	target string,
) (ASICSettings, error) {
	var settings ASICSettings
	if err := client.getJSON(ctx, target, "/api/system/asic", &settings); err != nil {
		return ASICSettings{}, fmt.Errorf("get ASIC settings: %w", err)
	}
	if settings.DefaultFrequency <= 0 || settings.DefaultFrequency > 10_000 {
		return ASICSettings{}, fmt.Errorf("get ASIC settings: invalid default frequency %d", settings.DefaultFrequency)
	}
	if !validCoreVoltage(settings.DefaultVoltage) {
		return ASICSettings{}, fmt.Errorf("get ASIC settings: invalid default voltage %d", settings.DefaultVoltage)
	}
	settings.FrequencyOptions = normalizedOptions(
		settings.FrequencyOptions,
		func(value int) bool { return value > 0 && value <= 10_000 },
	)
	settings.VoltageOptions = normalizedOptions(settings.VoltageOptions, validCoreVoltage)
	if len(settings.FrequencyOptions) == 0 || len(settings.VoltageOptions) == 0 {
		return ASICSettings{}, fmt.Errorf("get ASIC settings: firmware returned no tuning options")
	}
	if !containsOption(settings.FrequencyOptions, settings.DefaultFrequency) {
		return ASICSettings{}, fmt.Errorf(
			"get ASIC settings: default frequency %d is not advertised",
			settings.DefaultFrequency,
		)
	}
	if !containsOption(settings.VoltageOptions, settings.DefaultVoltage) {
		return ASICSettings{}, fmt.Errorf(
			"get ASIC settings: default voltage %d is not advertised",
			settings.DefaultVoltage,
		)
	}
	return settings, nil
}

type axeOperatingPointPatch struct {
	Frequency    int  `json:"frequency"`
	CoreVoltage  int  `json:"coreVoltage"`
	OverHeatMode *int `json:"overheat_mode,omitempty"`
}

type axeMiningPatch struct {
	StratumURL              string `json:"stratumURL"`
	StratumPort             int    `json:"stratumPort"`
	StratumUser             string `json:"stratumUser"`
	StratumPassword         string `json:"stratumPassword"`
	FallbackStratumURL      string `json:"fallbackStratumURL"`
	FallbackStratumPort     int    `json:"fallbackStratumPort"`
	FallbackStratumUser     string `json:"fallbackStratumUser"`
	FallbackStratumPassword string `json:"fallbackStratumPassword"`
}

// PatchOperatingPoint persists one complete advertised operating-point pair.
// The caller must restart and verify the device before treating it as active.
func (client *BitaxeClient) PatchOperatingPoint(
	ctx context.Context,
	point OperatingPoint,
	target string,
) error {
	if err := validateOperatingPoint(point); err != nil {
		return fmt.Errorf("patch operating point: %w", err)
	}
	return client.patch(ctx, target, axeOperatingPointPatch{
		Frequency:   point.Frequency,
		CoreVoltage: point.CoreVoltage,
	}, false)
}

// PatchOverheatRecovery persists one complete pair while clearing the firmware
// overheat flag. The caller must restart and verify the device.
func (client *BitaxeClient) PatchOverheatRecovery(
	ctx context.Context,
	point OperatingPoint,
	target string,
) error {
	if err := validateOperatingPoint(point); err != nil {
		return fmt.Errorf("patch overheat recovery: %w", err)
	}

	disabled := 0
	return client.patch(ctx, target, axeOperatingPointPatch{
		Frequency:    point.Frequency,
		CoreVoltage:  point.CoreVoltage,
		OverHeatMode: &disabled,
	}, false)
}

// PatchMiningConfiguration persists complete primary and fallback Stratum
// settings without exposing a secret-bearing payload type.
func (client *BitaxeClient) PatchMiningConfiguration(
	ctx context.Context,
	settings MiningSettings,
	primaryPassword string,
	fallbackPassword string,
	target string,
) error {
	if err := validateMiningSettings(settings); err != nil {
		return fmt.Errorf("patch mining configuration: %w", err)
	}
	if !settings.Enabled {
		return fmt.Errorf("patch mining configuration: mining is not enabled")
	}
	if err := validateResolvedPassword("primary", primaryPassword); err != nil {
		return fmt.Errorf("patch mining configuration: %w", err)
	}
	if err := validateResolvedPassword("fallback", fallbackPassword); err != nil {
		return fmt.Errorf("patch mining configuration: %w", err)
	}
	return client.patch(ctx, target, axeMiningPatch{
		StratumURL:              settings.Primary.Host,
		StratumPort:             settings.Primary.Port,
		StratumUser:             settings.Primary.User,
		StratumPassword:         primaryPassword,
		FallbackStratumURL:      settings.Fallback.Host,
		FallbackStratumPort:     settings.Fallback.Port,
		FallbackStratumUser:     settings.Fallback.User,
		FallbackStratumPassword: fallbackPassword,
	}, true)
}

// Restart requests an AxeOS system restart.
func (client *BitaxeClient) Restart(ctx context.Context, target string) error {
	requestURL, err := bitaxeURL(target, "/api/system/restart")
	if err != nil {
		return fmt.Errorf("restart Bitaxe: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, http.NoBody)
	if err != nil {
		return fmt.Errorf("restart Bitaxe: create request: %w", err)
	}
	response, err := client.do(request)
	if err != nil {
		return fmt.Errorf("restart Bitaxe: %w", err)
	}
	defer response.Body.Close()
	if err := requireSuccessfulStatusOnly(response); err != nil {
		return fmt.Errorf("restart Bitaxe: %w", err)
	}
	if err := discardBounded(response.Body); err != nil {
		return fmt.Errorf("restart Bitaxe: %w", err)
	}
	return nil
}

func validateOperatingPoint(point OperatingPoint) error {
	switch {
	case point.Frequency <= 0 || point.Frequency > 10_000:
		return fmt.Errorf("invalid frequency %d", point.Frequency)
	case !validCoreVoltage(point.CoreVoltage):
		return fmt.Errorf("invalid core voltage %d", point.CoreVoltage)
	default:
		return nil
	}
}

func validateResolvedPassword(name string, password string) error {
	if password == "" {
		return fmt.Errorf("%s password environment variable is empty", name)
	}
	if !utf8.ValidString(password) || len([]byte(password)) > 255 {
		return fmt.Errorf("%s password must be valid UTF-8 and at most 255 bytes", name)
	}
	return nil
}

func normalizedOptions(options []int, valid func(int) bool) []int {
	normalized := make([]int, 0, len(options))
	seen := make(map[int]struct{}, len(options))
	for _, option := range options {
		if valid == nil || !valid(option) {
			continue
		}
		if _, exists := seen[option]; exists {
			continue
		}
		seen[option] = struct{}{}
		normalized = append(normalized, option)
	}
	sort.Ints(normalized)
	return normalized
}

func containsOption(options []int, target int) bool {
	index := sort.SearchInts(options, target)
	return index < len(options) && options[index] == target
}

func (client *BitaxeClient) getJSON(
	ctx context.Context,
	target string,
	path string,
	destination any,
) error {
	requestURL, err := bitaxeURL(target, path)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	response, err := client.do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if err := requireSuccessfulStatus(response); err != nil {
		return err
	}
	if err := decodeResponse(response.Body, destination); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (client *BitaxeClient) patch(
	ctx context.Context,
	target string,
	patch any,
	secret bool,
) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("encode settings: %w", err)
	}
	if len(body) > maxAPIRequestSize {
		return fmt.Errorf("encoded settings exceed %d bytes", maxAPIRequestSize)
	}
	requestURL, err := bitaxeURL(target, "/api/system")
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPatch,
		requestURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create settings request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := client.do(request)
	if err != nil {
		return fmt.Errorf("patch Bitaxe: %w", err)
	}
	defer response.Body.Close()

	if secret {
		err = requireSuccessfulStatusOnly(response)
	} else {
		err = requireSuccessfulStatus(response)
	}
	if err != nil {
		return fmt.Errorf("patch Bitaxe: %w", err)
	}
	if err := discardBounded(response.Body); err != nil {
		return fmt.Errorf("patch Bitaxe: %w", err)
	}
	return nil
}

func (client *BitaxeClient) do(request *http.Request) (*http.Response, error) {
	if client == nil || client.httpClient == nil {
		return nil, fmt.Errorf("Bitaxe client is not initialized")
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, fmt.Errorf("Bitaxe returned an empty HTTP response")
	}
	return response, nil
}

func bitaxeURL(target string, path string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("Bitaxe target cannot be empty")
	}
	if strings.ContainsAny(target, "/\\?#") {
		return "", fmt.Errorf("invalid Bitaxe target %q", target)
	}
	return (&url.URL{Scheme: "http", Host: target, Path: path}).String(), nil
}

func requireSuccessfulStatus(response *http.Response) error {
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
	detail := strings.TrimSpace(string(body))
	status := response.Status
	if status == "" {
		status = fmt.Sprintf("%d", response.StatusCode)
	}
	if detail == "" {
		return fmt.Errorf("HTTP %s", status)
	}
	return fmt.Errorf("HTTP %s: %s", status, detail)
}

func requireSuccessfulStatusOnly(response *http.Response) error {
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	status := response.Status
	if status == "" {
		status = fmt.Sprintf("%d", response.StatusCode)
	}
	return fmt.Errorf("HTTP %s", status)
}

func discardBounded(body io.Reader) error {
	data, err := io.ReadAll(io.LimitReader(body, maxAPIResponseSize+1))
	if err != nil {
		return err
	}
	if len(data) > maxAPIResponseSize {
		return fmt.Errorf("response exceeds %d bytes", maxAPIResponseSize)
	}
	return nil
}

func decodeResponse(body io.Reader, destination any) error {
	data, err := io.ReadAll(io.LimitReader(body, maxAPIResponseSize+1))
	if err != nil {
		return err
	}
	if len(data) > maxAPIResponseSize {
		return fmt.Errorf("response exceeds %d bytes", maxAPIResponseSize)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return fmt.Errorf("response body is empty")
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return err
	}
	return nil
}

func validateInfo(info Info) error {
	_, macErr := normalizeMAC(info.MacAddr)
	switch {
	case strings.TrimSpace(info.Hostname) == "":
		return fmt.Errorf("hostname is empty")
	case strings.TrimSpace(info.Hostname) != info.Hostname || hasControl(info.Hostname):
		return fmt.Errorf("hostname is invalid")
	case macErr != nil:
		return macErr
	case strings.TrimSpace(info.Version) == "":
		return fmt.Errorf("firmware version is empty")
	case strings.TrimSpace(info.ASICModel) == "":
		return fmt.Errorf("ASIC model is empty")
	case strings.TrimSpace(info.BoardVersion) == "":
		return fmt.Errorf("board version is empty")
	case info.Frequency <= 0 || info.Frequency > 10_000:
		return fmt.Errorf("frequency %d is outside the accepted range", info.Frequency)
	case info.CoreVoltage < 0 || info.CoreVoltage > 10_000:
		return fmt.Errorf("core voltage %d is outside the accepted range", info.CoreVoltage)
	case !finite(info.CoreVoltageActual) || info.CoreVoltageActual < 0 ||
		info.CoreVoltageActual > 10_000:
		return fmt.Errorf("actual core voltage %.2f is outside the accepted range", info.CoreVoltageActual)
	case !finite(info.HashRate):
		return fmt.Errorf("hash rate %.2f is invalid", info.HashRate)
	case !finite(info.ExpectedHashRate):
		return fmt.Errorf("expected hash rate %.2f is invalid", info.ExpectedHashRate)
	case !finite(info.Temp) || info.Temp < -100 || info.Temp > 200:
		return fmt.Errorf("ASIC temperature %.2f is invalid", info.Temp)
	case !finite(info.VRTemp) || info.VRTemp < -100 || info.VRTemp > 200:
		return fmt.Errorf("VR temperature %.2f is invalid", info.VRTemp)
	case !finite(info.Power) || info.Power < 0 || info.Power > 10_000:
		return fmt.Errorf("power %.2f is invalid", info.Power)
	case info.UpTimeSeconds < 0:
		return fmt.Errorf("uptime %d is invalid", info.UpTimeSeconds)
	case info.OverHeatMode < 0:
		return fmt.Errorf("overheat mode %d is invalid", info.OverHeatMode)
	case info.StratumPort < 0 || info.StratumPort > 65535:
		return fmt.Errorf("primary Stratum port %d is invalid", info.StratumPort)
	case info.FallbackStratumPort < 0 || info.FallbackStratumPort > 65535:
		return fmt.Errorf("fallback Stratum port %d is invalid", info.FallbackStratumPort)
	case info.IsUsingFallbackStratum < 0 || info.IsUsingFallbackStratum > 1:
		return fmt.Errorf(
			"fallback Stratum status %d is invalid",
			info.IsUsingFallbackStratum,
		)
	case !finite(info.FanSpeed) || info.FanSpeed < 0 || info.FanSpeed > 100:
		return fmt.Errorf("fan speed %.2f is invalid", info.FanSpeed)
	case info.FanRPM < 0:
		return fmt.Errorf("fan RPM %d is invalid", info.FanRPM)
	}
	return nil
}

func normalizeMAC(value string) (string, error) {
	address, err := net.ParseMAC(value)
	if err != nil || len(address) != 6 {
		return "", fmt.Errorf("MAC address %q is invalid", value)
	}
	return address.String(), nil
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

type SystemInfoClient interface {
	GetSystemInfo(context.Context, string) (Info, error)
}

type scanJob struct {
	ip string
}

type scanResult struct {
	miner DiscoveredMiner
}

// DiscoveredMiner keeps validated identity and telemetry attached to its
// currently observed IP address.
type DiscoveredMiner struct {
	IP   string
	Info Info
}

// ScanNetwork probes each local IPv4 /24 with a fixed worker limit. Probe
// failures are expected and ignored; interface discovery and cancellation are
// returned to the caller.
func ScanNetwork(
	ctx context.Context,
	hostnames map[string]bool,
	settingsFile SettingsFile,
	client SystemInfoClient,
) ([]DiscoveredMiner, error) {
	if client == nil {
		return nil, fmt.Errorf("scan network: system-info client is nil")
	}
	prefixes, err := localIPv4Prefixes()
	if err != nil {
		return nil, fmt.Errorf("scan network: %w", err)
	}

	log.Printf("Scanning network for Bitaxes...")
	candidateCount := len(prefixes) * (dhcpEnd - dhcpStart + 1)
	jobs := make(chan scanJob, candidateCount)
	results := make(chan scanResult, candidateCount)
	for _, prefix := range prefixes {
		log.Printf("Scanning %s.0/24", prefix)
		for host := dhcpStart; host <= dhcpEnd; host++ {
			select {
			case jobs <- scanJob{ip: fmt.Sprintf("%s.%d", prefix, host)}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	close(jobs)

	workerCount := scanWorkerLimit
	if workerCount > candidateCount {
		workerCount = candidateCount
	}

	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				probeMiner(ctx, job.ip, hostnames, settingsFile, client, results)
			}
		}()
	}
	workers.Wait()
	close(results)

	var discovered []scanResult
	for result := range results {
		discovered = append(discovered, result)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	miners, err := canonicalDiscovered(discovered)
	if err != nil {
		return nil, fmt.Errorf("scan network: %w", err)
	}
	for _, miner := range miners {
		log.Printf("Found Bitaxe: %s %s", miner.IP, miner.Info.Hostname)
	}
	return miners, nil
}

func canonicalDiscovered(results []scanResult) ([]DiscoveredMiner, error) {
	found := make(map[string]DiscoveredMiner, len(results))
	for _, result := range results {
		miner := result.miner
		if existing, ok := found[miner.Info.MacAddr]; ok && existing.IP != miner.IP {
			return nil, fmt.Errorf(
				"MAC %s was discovered at both %s and %s",
				miner.Info.MacAddr,
				existing.IP,
				miner.IP,
			)
		}
		found[miner.Info.MacAddr] = miner
	}
	miners := make([]DiscoveredMiner, 0, len(found))
	for _, miner := range found {
		miners = append(miners, miner)
	}
	sort.Slice(miners, func(left int, right int) bool {
		if miners[left].Info.MacAddr != miners[right].Info.MacAddr {
			return miners[left].Info.MacAddr < miners[right].Info.MacAddr
		}
		return bytes.Compare(
			net.ParseIP(miners[left].IP).To4(),
			net.ParseIP(miners[right].IP).To4(),
		) < 0
	})
	return miners, nil
}

func probeMiner(
	ctx context.Context,
	ip string,
	hostnames map[string]bool,
	settingsFile SettingsFile,
	client SystemInfoClient,
	results chan<- scanResult,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("Recovered panic while probing %s\n%s", ip, debug.Stack())
		}
	}()

	info, err := client.GetSystemInfo(ctx, ip)
	if err != nil {
		return
	}
	settings, err := settingsFile.ForHost(info.Hostname)
	if err != nil {
		log.Printf("Ignoring %s due to invalid settings: %s", info.Hostname, err)
		return
	}
	if settings.Skip {
		log.Printf("Skipping Bitaxe: %s", info.Hostname)
		return
	}
	_, scanAll := hostnames["all"]
	_, scanHostname := hostnames[info.Hostname]
	if len(hostnames) > 0 && !scanAll && !scanHostname {
		return
	}

	select {
	case results <- scanResult{miner: DiscoveredMiner{IP: ip, Info: info}}:
	case <-ctx.Done():
	}
}

func localIPv4Prefixes() ([]string, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	unique := make(map[string]struct{})
	for _, address := range addresses {
		ipNetwork, ok := address.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipNetwork.IP.To4()
		if ip == nil || ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() {
			continue
		}
		unique[fmt.Sprintf("%d.%d.%d", ip[0], ip[1], ip[2])] = struct{}{}
	}
	if len(unique) == 0 {
		return nil, errors.New("no usable local IPv4 interfaces found")
	}

	prefixes := make([]string, 0, len(unique))
	for prefix := range unique {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	return prefixes, nil
}
