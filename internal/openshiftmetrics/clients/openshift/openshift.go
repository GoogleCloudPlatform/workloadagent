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

// Package openshift provides a client for interacting with Openshift clusters.
package openshift

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"k8s.io/client-go/rest"
)

// Client is a client for interacting with Openshift clusters.
type Client struct {
	client *http.Client
	config *rest.Config
}

// New creates a new Client for interacting with Openshift clusters.
func New(config *rest.Config) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, err
	}
	return &Client{client: httpClient, config: config}, nil
}

// GetClusterVersion returns the cluster version data of the Openshift cluster.
//
// TODO: make this compatible with the Openshift client interface.
func (c *Client) GetClusterVersion() (*ClusterVersionResponse, error) {
	uri := "/apis/config.openshift.io/v1/clusterversions"
	url := fmt.Sprintf("%s%s", c.config.Host, uri)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// TODO: implement retry logic.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get cluster version: %v", string(body))
	}
	var clusterVersionResponse ClusterVersionResponse
	if err := json.Unmarshal(body, &clusterVersionResponse); err != nil {
		return nil, err
	}
	return &clusterVersionResponse, nil
}
