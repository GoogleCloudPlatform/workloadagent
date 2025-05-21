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

// Package metricutils provides utility functions for metrics collection.
package metricutils

import (
	"context"
	"strings"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

// ZonesFromIPs returns the zones for the given IPs.
func ZonesFromIPs(ctx context.Context, ips []string, netLookupAddr func(ip string) ([]string, error)) []string {
	var zones []string
	for _, ip := range ips {
		names, err := netLookupAddr(ip)
		if err != nil {
			log.CtxLogger(ctx).Debugf("Failed to lookup address: %v", err)
			continue
		}
		if len(names) == 0 {
			log.CtxLogger(ctx).Debugf("No hostname found for IP: %s", ip)
			continue
		}
		for _, name := range names {
			// name is something like "name.us-central1-a.c.gce-performance-manual.internal."
			// and we want the zone.
			fields := strings.Split(name, ".")
			if len(fields) > 1 {
				zones = append(zones, fields[1])
			}
		}
	}
	return zones
}
