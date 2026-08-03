package lib

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func testBitaxeClient(roundTrip roundTripFunc) *BitaxeClient {
	return newBitaxeClient(&http.Client{Transport: roundTrip})
}

func successfulResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func enabledMiningSettings() MiningSettings {
	return MiningSettings{
		Enabled: true,
		Primary: PoolSettings{
			Host:        "pool.example.net",
			Port:        3333,
			User:        "worker-primary",
			PasswordEnv: "BITAGNIS_PRIMARY_PASSWORD",
		},
		Fallback: PoolSettings{
			Host:        "fallback.example.net",
			Port:        4444,
			User:        "worker-fallback",
			PasswordEnv: "BITAGNIS_FALLBACK_PASSWORD",
		},
	}
}

func validInfoJSON() string {
	return `{
		"version":"v2.8.1",
		"ASICModel":"BM1370",
		"boardVersion":"601",
		"hostname":"bitaxe-alpha",
		"macAddr":"AA:BB:CC:DD:EE:FF",
		"frequency":400,
		"coreVoltage":1100,
		"coreVoltageActual":1085,
		"hashRate":799.3,
		"expectedHashrate":816,
		"errorPercentage":1.25,
		"sharesAccepted":384,
		"sharesRejected":2,
		"fanspeed":100,
		"fanrpm":8450,
		"temp":62.1,
		"vrTemp":49,
		"power":14.8,
		"uptimeSeconds":2341,
		"overheat_mode":0,
		"stratumURL":"pool.example.net",
		"stratumPort":3333,
		"stratumUser":"worker-primary",
		"fallbackStratumURL":"fallback.example.net",
		"fallbackStratumPort":4444,
		"fallbackStratumUser":"worker-fallback",
		"isUsingFallbackStratum":0
	}`
}

func TestOperatingPointPatchesAreCompleteAndDistinct(t *testing.T) {
	var patches []map[string]int
	client := testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPatch || request.URL.Path != "/api/system" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		var patch map[string]int
		if err := json.NewDecoder(request.Body).Decode(&patch); err != nil {
			t.Errorf("decode request: %v", err)
		}
		patches = append(patches, patch)
		return successfulResponse(""), nil
	})

	point := OperatingPoint{Frequency: 490, CoreVoltage: 1060}
	if err := client.PatchOperatingPoint(context.Background(), point, "bitaxe.test"); err != nil {
		t.Fatalf("PatchOperatingPoint returned an error: %v", err)
	}
	if err := client.PatchOverheatRecovery(context.Background(), point, "bitaxe.test"); err != nil {
		t.Fatalf("PatchOverheatRecovery returned an error: %v", err)
	}
	if len(patches) != 2 ||
		len(patches[0]) != 2 ||
		patches[0]["frequency"] != 490 ||
		patches[0]["coreVoltage"] != 1060 {
		t.Fatalf("operating-point patches = %+v", patches)
	}
	if _, exists := patches[0]["overheat_mode"]; exists {
		t.Fatal("normal patch unexpectedly cleared overheat mode")
	}
	if len(patches[1]) != 3 ||
		patches[1]["frequency"] != 490 ||
		patches[1]["coreVoltage"] != 1060 ||
		patches[1]["overheat_mode"] != 0 {
		t.Fatalf("recovery patch = %+v", patches[1])
	}
}

func TestMiningPatchSendsExactCompletePayload(t *testing.T) {
	const primarySecret = "synthetic-primary-secret"
	const fallbackSecret = "synthetic-fallback-secret"
	var patch map[string]any
	client := testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(request.Body).Decode(&patch); err != nil {
			t.Errorf("decode request: %v", err)
		}
		return successfulResponse(""), nil
	})
	err := client.PatchMiningConfiguration(
		context.Background(),
		enabledMiningSettings(),
		primarySecret,
		fallbackSecret,
		"bitaxe.test",
	)
	if err != nil {
		t.Fatalf("PatchMiningConfiguration returned an error: %v", err)
	}
	want := map[string]any{
		"stratumURL":              "pool.example.net",
		"stratumPort":             float64(3333),
		"stratumUser":             "worker-primary",
		"stratumPassword":         primarySecret,
		"fallbackStratumURL":      "fallback.example.net",
		"fallbackStratumPort":     float64(4444),
		"fallbackStratumUser":     "worker-fallback",
		"fallbackStratumPassword": fallbackSecret,
	}
	if fmtJSON(patch) != fmtJSON(want) {
		t.Fatalf("mining patch = %s, want %s", fmtJSON(patch), fmtJSON(want))
	}
}

func fmtJSON(value any) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func TestMutationValidationHappensBeforeRequest(t *testing.T) {
	requests := 0
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		requests++
		return successfulResponse(""), nil
	})
	if err := client.PatchOperatingPoint(
		context.Background(),
		OperatingPoint{Frequency: 0, CoreVoltage: 1100},
		"bitaxe.test",
	); err == nil {
		t.Fatal("invalid operating point was accepted")
	}
	if err := client.PatchOperatingPoint(
		context.Background(),
		OperatingPoint{Frequency: 50, CoreVoltage: 1000},
		"bitaxe.test",
	); err == nil {
		t.Fatal("firmware emergency sentinel was accepted")
	}
	if err := client.PatchMiningConfiguration(
		context.Background(),
		enabledMiningSettings(),
		strings.Repeat("x", 256),
		"fallback",
		"bitaxe.test",
	); err == nil {
		t.Fatal("oversized password was accepted")
	}
	if requests != 0 {
		t.Fatalf("HTTP requests = %d, want none", requests)
	}
}

func TestSecretBearingHTTPFailureIsStatusOnly(t *testing.T) {
	const sentinel = "synthetic-secret-must-not-escape"
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Status:     "400 Bad Request",
			Body:       io.NopCloser(strings.NewReader("rejected " + sentinel)),
			Header:     make(http.Header),
		}, nil
	})
	err := client.PatchMiningConfiguration(
		context.Background(),
		enabledMiningSettings(),
		sentinel,
		"fallback-secret",
		"bitaxe.test",
	)
	if err == nil || !strings.Contains(err.Error(), "400 Bad Request") {
		t.Fatalf("error = %v, want HTTP status", err)
	}
	if strings.Contains(err.Error(), sentinel) || strings.Contains(err.Error(), "rejected") {
		t.Fatalf("secret-bearing error included response detail: %v", err)
	}
}

func TestRestartUsesPostAndBoundsResponse(t *testing.T) {
	client := testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/system/restart" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		return successfulResponse(strings.Repeat("x", maxAPIResponseSize+1)), nil
	})
	err := client.Restart(context.Background(), "bitaxe.test")
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("restart error = %v, want response size error", err)
	}
}

func TestClientRejectsRedirects(t *testing.T) {
	var requests atomic.Int32
	client := testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		if requests.Load() > 1 {
			t.Fatal("client followed redirect")
		}
		return &http.Response{
			StatusCode: http.StatusFound,
			Status:     "302 Found",
			Header:     http.Header{"Location": {"http://other.test/api/system/info"}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	})
	_, err := client.GetSystemInfo(context.Background(), "bitaxe.test")
	if err == nil || !strings.Contains(err.Error(), "redirects are not allowed") {
		t.Fatalf("redirect error = %v", err)
	}
}

func TestGetASICSettingsReturnsNormalizedAdvertisedGrid(t *testing.T) {
	client := testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/api/system/asic" {
			t.Errorf("path = %q, want /api/system/asic", request.URL.Path)
		}
		return successfulResponse(`{
			"ASICModel":"BM1370",
			"defaultFrequency":525,
			"frequencyOptions":[625,400,525,490,525],
			"defaultVoltage":1150,
			"voltageOptions":[1250,1000,1150,1060,1150]
		}`), nil
	})

	settings, err := client.GetASICSettings(context.Background(), "bitaxe.test")
	if err != nil {
		t.Fatalf("GetASICSettings returned an error: %v", err)
	}
	if got := settings.FrequencyOptions; fmtJSON(got) != "[400,490,525,625]" {
		t.Fatalf("frequency options = %v", got)
	}
	if got := settings.VoltageOptions; fmtJSON(got) != "[1000,1060,1150,1250]" {
		t.Fatalf("voltage options = %v", got)
	}
}

func TestGetASICSettingsRejectsInvalidOptions(t *testing.T) {
	for _, body := range []string{
		`{"ASICModel":"BM1370","defaultFrequency":525,"frequencyOptions":[400,0,525],"defaultVoltage":1150,"voltageOptions":[1000,1150]}`,
		`{"ASICModel":"BM1370","defaultFrequency":525,"frequencyOptions":[400,525],"defaultVoltage":1150,"voltageOptions":[1000,2100]}`,
	} {
		client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
			return successfulResponse(body), nil
		})
		if _, err := client.GetASICSettings(context.Background(), "bitaxe.test"); err == nil {
			t.Fatalf("invalid ASIC options were accepted: %s", body)
		}
	}
}

func TestGetSystemInfoExtractsAndValidatesMutationFields(t *testing.T) {
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		return successfulResponse(validInfoJSON()), nil
	})
	info, err := client.GetSystemInfo(context.Background(), "bitaxe.test")
	if err != nil {
		t.Fatalf("GetSystemInfo returned an error: %v", err)
	}
	if info.MacAddr != "aa:bb:cc:dd:ee:ff" ||
		info.Version != "v2.8.1" ||
		info.ASICModel != "BM1370" ||
		info.BoardVersion != "601" ||
		info.StratumURL != "pool.example.net" ||
		info.FallbackStratumPort != 4444 ||
		info.ExpectedHashRate != 816 {
		t.Fatalf("system info = %+v", info)
	}
}

func TestGetSystemInfoValidatesHTTPAndPayload(t *testing.T) {
	tests := []struct {
		name      string
		response  *http.Response
		transport error
		wantError string
	}{
		{name: "valid", response: successfulResponse(validInfoJSON())},
		{
			name: "HTTP error",
			response: &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Body:       io.NopCloser(strings.NewReader("rebooting")),
			},
			wantError: "503 Service Unavailable",
		},
		{
			name:      "malformed JSON",
			response:  successfulResponse(`{"hostname":`),
			wantError: "decode response",
		},
		{
			name:      "missing identity",
			response:  successfulResponse(`{"frequency":525}`),
			wantError: "hostname is empty",
		},
		{
			name:      "invalid error percentage",
			response:  successfulResponse(strings.Replace(validInfoJSON(), `"errorPercentage":1.25`, `"errorPercentage":101`, 1)),
			wantError: "error percentage is invalid",
		},
		{
			name:      "negative hash rate",
			response:  successfulResponse(strings.Replace(validInfoJSON(), `"hashRate":799.3`, `"hashRate":-1`, 1)),
			wantError: "hash rate",
		},
		{
			name:      "transport error",
			transport: errors.New("network down"),
			wantError: "network down",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
				return test.response, test.transport
			})
			_, err := client.GetSystemInfo(context.Background(), "bitaxe.test")
			if test.wantError == "" && err != nil {
				t.Fatalf("GetSystemInfo returned an error: %v", err)
			}
			if test.wantError != "" &&
				(err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestGetSystemInfoRejectsOversizedResponseAndHonorsCancellation(t *testing.T) {
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		return successfulResponse(strings.Repeat(" ", maxAPIResponseSize+1)), nil
	})
	if _, err := client.GetSystemInfo(context.Background(), "bitaxe.test"); err == nil ||
		!strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized response error = %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	client = testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		return nil, request.Context().Err()
	})
	if _, err := client.GetSystemInfo(cancelled, "bitaxe.test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}

type panicInfoClient struct{}

func (panicInfoClient) GetSystemInfo(context.Context, string) (Info, error) {
	panic("probe exploded")
}

func TestProbeMinerContainsClientPanic(t *testing.T) {
	results := make(chan scanResult, 1)
	probeMiner(
		context.Background(),
		"192.0.2.10",
		map[string]bool{"all": true},
		SettingsFile{Defaults: withDefaults(Settings{})},
		panicInfoClient{},
		results,
	)
	if len(results) != 0 {
		t.Fatal("panicking probe unexpectedly produced a result")
	}
}

func TestCanonicalDiscoveryOrdersByMACAndRejectsAmbiguousIP(t *testing.T) {
	results := []scanResult{
		{miner: DiscoveredMiner{
			IP:   "192.0.2.20",
			Info: Info{MacAddr: "aa:bb:cc:dd:ee:20"},
		}},
		{miner: DiscoveredMiner{
			IP:   "192.0.2.10",
			Info: Info{MacAddr: "00:11:22:33:44:55"},
		}},
	}
	miners, err := canonicalDiscovered(results)
	if err != nil {
		t.Fatalf("canonical discovery: %v", err)
	}
	if len(miners) != 2 ||
		miners[0].Info.MacAddr != "00:11:22:33:44:55" ||
		miners[1].Info.MacAddr != "aa:bb:cc:dd:ee:20" {
		t.Fatalf("canonical miners = %+v", miners)
	}
	results = append(results, scanResult{miner: DiscoveredMiner{
		IP:   "192.0.2.21",
		Info: Info{MacAddr: "aa:bb:cc:dd:ee:20"},
	}})
	if _, err := canonicalDiscovered(results); err == nil {
		t.Fatal("one MAC at two IPs was accepted")
	}
}

func TestRedirectCancellationDoesNotWaitForTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	client := testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})
	started := time.Now()
	_, err := client.GetSystemInfo(ctx, "bitaxe.test")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("bounded cancellation = %v after %s", err, time.Since(started))
	}
}
