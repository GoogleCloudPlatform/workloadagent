/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ipinfo

import (
	"errors"
	"testing"

	"context"

	"github.com/google/go-cmp/cmp"
)

func TestZoneFromHost(t *testing.T) {
	tests := []struct {
		name string
		host string
		want string
	}{
		{
			name: "ValidHost",
			host: "hostname.us-central1-a.c.fake-project.internal.",
			want: "us-central1-a",
		},
		{
			name: "InvalidHost",
			host: "invalid-host",
			want: "",
		},
		{
			name: "EmptyHost",
			host: "",
			want: "",
		},
		{
			name: "HostWithExtraDots",
			host: "hostname.us-central1-a.extra.dots.c.fake-project.internal.",
			want: "us-central1-a",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ZoneFromHost(context.Background(), tc.host)
			if got != tc.want {
				t.Errorf("ZoneFromHost() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestZonesFromHosts(t *testing.T) {
	tests := []struct {
		name  string
		hosts []string
		want  []string
	}{
		{
			name:  "ValidHosts",
			hosts: []string{"hostname.us-central1-a.c.fake-project.internal.", "hostname.europe-west1-b.c.fake-project.internal."},
			want:  []string{"us-central1-a", "europe-west1-b"},
		},
		{
			name:  "InvalidHosts",
			hosts: []string{"invalid-host", "another-invalid-host"},
			want:  nil,
		},
		{
			name:  "MixedHosts",
			hosts: []string{"hostname.us-central1-a.c.fake-project.internal.", "invalid-host"},
			want:  []string{"us-central1-a"},
		},
		{
			name:  "EmptyHosts",
			hosts: []string{},
			want:  nil,
		},
		{
			name: "NilHosts",
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ZonesFromHosts(context.Background(), tc.hosts)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("ZonesFromHosts() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestZoneFromIP(t *testing.T) {
	tests := []struct {
		name            string
		ip              string
		lookupAddrValue []string
		lookupAddrErr   error
		want            string
	}{
		{
			name:            "ValidIP",
			ip:              "1.2.3.4",
			lookupAddrValue: []string{"hostname.us-central1-a.c.fake-project.internal."},
			want:            "us-central1-a",
		},
		{
			name:            "MultipleValidHostnames",
			ip:              "1.2.3.4",
			lookupAddrValue: []string{"hostname1.us-central1-a.c.fake-project.internal.", "hostname2.us-central1-b.c.fake-project.internal."},
			want:            "us-central1-a", // Should return the first valid zone
		},
		{
			name:            "LookupError",
			ip:              "1.2.3.4",
			lookupAddrValue: []string{"valueThatShouldNotBeUsedBecauseOfError"},
			lookupAddrErr:   errors.New("fake error"),
			want:            "",
		},
		{
			name:            "NoHostname",
			ip:              "1.2.3.4",
			lookupAddrValue: []string{},
			want:            "",
		},
		{
			name:            "MultipleHostnamesTakeFirstValid",
			ip:              "1.2.3.4",
			lookupAddrValue: []string{"invalid-hostname", "hostname1.us-central1-a.c.fake-project.internal.", "hostname2.us-central1-b.c.fake-project.internal."},
			want:            "us-central1-a", // Should return the first valid zone
		},
		{
			name:            "InvalidHostname",
			ip:              "1.2.3.4",
			lookupAddrValue: []string{"invalid-hostname"},
			want:            "",
		},
		{
			name: "EmptyResponse",
			ip:   "1.2.3.4",
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockLookupAddr := func(ip string) ([]string, error) {
				return tc.lookupAddrValue, tc.lookupAddrErr
			}
			got := ZoneFromIP(context.Background(), tc.ip, mockLookupAddr)
			if got != tc.want {
				t.Errorf("ZoneFromIP() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestZonesFromIPs(t *testing.T) {
	tests := []struct {
		name            string
		ips             []string
		lookupAddrValue map[string][]string
		lookupAddrErr   map[string]error
		want            []string
	}{
		{
			name: "ValidIPs",
			ips:  []string{"1.2.3.4", "5.6.7.8"},
			lookupAddrValue: map[string][]string{
				"1.2.3.4": {"hostname.us-central1-a.c.fake-project.internal."},
				"5.6.7.8": {"hostname.europe-west1-b.c.fake-project.internal."},
			},
			want: []string{"us-central1-a", "europe-west1-b"},
		},
		{
			name: "LookupError",
			ips:  []string{"1.2.3.4", "5.6.7.8"},
			lookupAddrErr: map[string]error{
				"1.2.3.4": errors.New("fake error"),
			},
			lookupAddrValue: map[string][]string{
				"5.6.7.8": {"hostname.europe-west1-b.c.fake-project.internal."},
			},
			want: []string{"europe-west1-b"},
		},
		{
			name: "NoHostname",
			ips:  []string{"1.2.3.4", "5.6.7.8"},
			lookupAddrValue: map[string][]string{
				"1.2.3.4": {},
				"5.6.7.8": {"hostname.europe-west1-b.c.fake-project.internal."},
			},
			want: []string{"europe-west1-b"},
		},
		{
			name: "EmptyIPList",
			ips:  []string{},
			want: nil,
		},
		{
			name: "MixedResults",
			ips:  []string{"1.2.3.4", "5.6.7.8", "9.10.11.12"},
			lookupAddrValue: map[string][]string{
				"1.2.3.4": {"hostname.us-central1-a.c.fake-project.internal."},
				"5.6.7.8": {},
			},
			lookupAddrErr: map[string]error{
				"5.6.7.8": errors.New("fake error"),
			},
			want: []string{"us-central1-a"},
		},
		{
			name: "MultipleHostnamesTakeFirstValid",
			ips:  []string{"1.2.3.4"},
			lookupAddrValue: map[string][]string{
				"1.2.3.4": {"hostname1.us-central1-a.c.fake-project.internal.", "hostname2.us-central1-b.c.fake-project.internal."},
			},
			want: []string{"us-central1-a"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockLookupAddr := func(ip string) ([]string, error) {
				if err, ok := tc.lookupAddrErr[ip]; ok {
					return nil, err
				}
				return tc.lookupAddrValue[ip], nil
			}

			got := ZonesFromIPs(context.Background(), tc.ips, mockLookupAddr)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("ZonesFromIPs() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}
