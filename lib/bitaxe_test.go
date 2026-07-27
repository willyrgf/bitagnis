package lib

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
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

func TestSetOperatingPointAlwaysSendsFrequencyAndVoltage(t *testing.T) {
	var patch map[string]int
	client := testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPatch {
			t.Errorf("method = %q, want PATCH", request.Method)
		}
		if err := json.NewDecoder(request.Body).Decode(&patch); err != nil {
			t.Errorf("decode request: %v", err)
		}
		return successfulResponse(""), nil
	})

	point := OperatingPoint{Frequency: 490, CoreVoltage: 1060}
	if err := client.SetOperatingPoint(context.Background(), point, "bitaxe.test"); err != nil {
		t.Fatalf("SetOperatingPoint returned an error: %v", err)
	}
	if patch["frequency"] != 490 || patch["coreVoltage"] != 1060 {
		t.Fatalf("patch = %+v, want complete operating point", patch)
	}
	if _, exists := patch["overheat_mode"]; exists {
		t.Fatal("normal operating-point patch unexpectedly cleared overheat mode")
	}
}

func TestRecoverOperatingPointClearsOverheatAtomically(t *testing.T) {
	var patch map[string]int
	client := testBitaxeClient(func(request *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(request.Body).Decode(&patch); err != nil {
			t.Errorf("decode request: %v", err)
		}
		return successfulResponse(""), nil
	})

	point := OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	if err := client.RecoverOperatingPoint(
		context.Background(),
		point,
		"bitaxe.test",
	); err != nil {
		t.Fatalf("RecoverOperatingPoint returned an error: %v", err)
	}
	if patch["frequency"] != 400 ||
		patch["coreVoltage"] != 1000 ||
		patch["overheat_mode"] != 0 {
		t.Fatalf("recovery patch = %+v", patch)
	}
}

func TestOperatingPointRejectsInvalidPairBeforeRequest(t *testing.T) {
	requests := 0
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		requests++
		return successfulResponse(""), nil
	})

	err := client.SetOperatingPoint(
		context.Background(),
		OperatingPoint{Frequency: 0, CoreVoltage: 1100},
		"bitaxe.test",
	)
	if err == nil {
		t.Fatal("SetOperatingPoint returned nil, want validation error")
	}
	if requests != 0 {
		t.Fatalf("HTTP requests = %d, want none", requests)
	}
}

func TestOperatingPointReportsHTTPFailure(t *testing.T) {
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Status:     "400 Bad Request",
			Body:       io.NopCloser(strings.NewReader("invalid settings")),
			Header:     make(http.Header),
		}, nil
	})
	err := client.SetOperatingPoint(
		context.Background(),
		OperatingPoint{Frequency: 525, CoreVoltage: 1150},
		"bitaxe.test",
	)
	if err == nil || !strings.Contains(err.Error(), "400 Bad Request") {
		t.Fatalf("error = %v, want HTTP status", err)
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
	if got := settings.FrequencyOptions; len(got) != 4 ||
		got[0] != 400 || got[1] != 490 || got[2] != 525 || got[3] != 625 {
		t.Fatalf("frequency options = %v", got)
	}
	if got := settings.VoltageOptions; len(got) != 4 ||
		got[0] != 1000 || got[1] != 1060 || got[2] != 1150 || got[3] != 1250 {
		t.Fatalf("voltage options = %v", got)
	}
}

func TestGetASICSettingsRejectsIncompleteGrid(t *testing.T) {
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		return successfulResponse(
			`{"defaultFrequency":525,"frequencyOptions":[400],"defaultVoltage":1150,"voltageOptions":[1000,1150]}`,
		), nil
	})
	_, err := client.GetASICSettings(context.Background(), "bitaxe.test")
	if err == nil || !strings.Contains(err.Error(), "default frequency") {
		t.Fatalf("error = %v, want default-grid validation", err)
	}
}

func TestGetSystemInfoExtractsOptimizerTelemetry(t *testing.T) {
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		return successfulResponse(`{
			"hostname":"mineira",
			"macAddr":"aa:bb",
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
			"uptimeSeconds":2341
		}`), nil
	})
	info, err := client.GetSystemInfo(context.Background(), "bitaxe.test")
	if err != nil {
		t.Fatalf("GetSystemInfo returned an error: %v", err)
	}
	if info.ExpectedHashRate != 816 ||
		info.CoreVoltageActual != 1085 ||
		info.ErrorPercentage == nil ||
		*info.ErrorPercentage != 1.25 ||
		info.SharesAccepted != 384 ||
		info.FanRPM != 8450 {
		t.Fatalf("optimizer telemetry = %+v", info)
	}
}

func TestGetSystemInfoValidatesHTTPAndPayload(t *testing.T) {
	tests := []struct {
		name      string
		response  *http.Response
		transport error
		wantError string
	}{
		{
			name: "valid without optional telemetry",
			response: successfulResponse(
				`{"hostname":"mineira","macAddr":"aa:bb","frequency":525,"coreVoltage":1150,"hashRate":1248,"temp":55}`,
			),
		},
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
			name: "unavailable hash sentinel remains usable for safety",
			response: successfulResponse(
				`{"hostname":"x","macAddr":"y","frequency":525,"coreVoltage":1150,"hashRate":-1,"temp":71,"power":20}`,
			),
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

func TestGetSystemInfoRejectsOversizedResponse(t *testing.T) {
	client := testBitaxeClient(func(_ *http.Request) (*http.Response, error) {
		return successfulResponse(strings.Repeat(" ", maxAPIResponseSize+1)), nil
	})
	_, err := client.GetSystemInfo(context.Background(), "bitaxe.test")
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v, want response size error", err)
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
