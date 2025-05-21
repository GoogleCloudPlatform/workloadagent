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

package metricutils

import (
	"errors"
	"testing"

	"context"

	"github.com/google/go-cmp/cmp"
)

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
				"9.10.11.12": errors.New("fake error"),
			},
			want: []string{"us-central1-a"},
		},
		{
			name: "MultipleHostnames",
			ips:  []string{"1.2.3.4"},
			lookupAddrValue: map[string][]string{
				"1.2.3.4": {"hostname1.us-central1-a.c.fake-project.internal.", "hostname2.us-central1-b.c.fake-project.internal."},
			},
			want: []string{"us-central1-a", "us-central1-b"},
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
