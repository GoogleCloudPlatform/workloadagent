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

// Package ipinfo provides utility functions for translating IP information.
package ipinfo

import (
	"context"
	"strings"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

// ZoneFromHost returns the zone for the given host.
func ZoneFromHost(ctx context.Context, host string) string {
	// host is something like "name.us-central1-a.c.gce-performance-manual.internal."
	// and we want the zone.
	fields := strings.Split(host, ".")
	if len(fields) > 1 {
		return fields[1]
	}
	return ""
}

// ZonesFromHosts returns the zones for the given hosts.
func ZonesFromHosts(ctx context.Context, hosts []string) []string {
	var zones []string
	for _, host := range hosts {
		zone := ZoneFromHost(ctx, host)
		if zone != "" {
			zones = append(zones, zone)
		}
	}
	return zones
}

// ZoneFromIP returns the zone for the given IP.
func ZoneFromIP(ctx context.Context, ip string, netLookupAddr func(ip string) ([]string, error)) string {
	names, err := netLookupAddr(ip)
	if err != nil {
		log.CtxLogger(ctx).Debugf("Failed to lookup address: %v", err)
		return ""
	}
	if len(names) == 0 {
		log.CtxLogger(ctx).Debugf("No hostname found for IP: %s", ip)
		return ""
	}
	for _, name := range names {
		zone := ZoneFromHost(ctx, name)
		if zone != "" {
			return zone
		}
	}
	return ""
}

// ZonesFromIPs returns the zones for the given IPs.
func ZonesFromIPs(ctx context.Context, ips []string, netLookupAddr func(ip string) ([]string, error)) []string {
	var zones []string
	for _, ip := range ips {
		zone := ZoneFromIP(ctx, ip, netLookupAddr)
		if zone != "" {
			zones = append(zones, zone)
		}
	}
	return zones
}
