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

// Package cliconfig defines the types used for the Google Cloud Agent for Compute Workloads CLI configuration.
// It also resolves the cyclic dependency between the top-level configure command and the various
// workload-specific subcommands.
package cliconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// Marshaller defines an interface for marshalling protobuf messages.
// This allows injecting different implementations for testing.
type Marshaller interface {
	Marshal(proto.Message) ([]byte, error)
}

// WriteConfigFile abstracts os.WriteFile function for testability.
type WriteConfigFile func(string, []byte, os.FileMode) error

// Configure holds the configuration state for the CLI.
type Configure struct {
	Configuration           *cpb.Configuration
	Path                    string
	OracleConfigModified    bool
	SQLServerConfigModified bool
	RedisConfigModified     bool
	MySQLConfigModified     bool

	// Injected dependencies (unexported)
	marshaller Marshaller
	fileWriter WriteConfigFile
}

// DefaultProtoMarshaller adapts protojson.Marshal to the Marshaller interface.
type DefaultProtoMarshaller struct {
	Options protojson.MarshalOptions
}

// Marshal implements the Marshaller interface using protojson.
func (m *DefaultProtoMarshaller) Marshal(msg proto.Message) ([]byte, error) {
	return m.Options.Marshal(msg)
}

// NewConfigure creates a new Config, injecting dependencies.
// If nil is passed for marshaller or fileWriter, defaults (real implementations) are used.
func NewConfigure(path string, m Marshaller, fw WriteConfigFile) *Configure {
	if m == nil {
		m = &DefaultProtoMarshaller{
			Options: protojson.MarshalOptions{
				UseProtoNames:   true,
				EmitUnpopulated: true,
			},
		}
	}
	if fw == nil {
		fw = os.WriteFile
	}

	return &Configure{
		Path:       path,
		marshaller: m,
		fileWriter: fw,
	}
}

// WriteFile writes the configuration using injected dependencies.
func (c *Configure) WriteFile(ctx context.Context) error {
	if c.Configuration == nil {
		return fmt.Errorf("configuration is nil")
	}

	clonedCfg := proto.Clone(c.Configuration).(*cpb.Configuration)

	fileBytes, err := c.marshaller.Marshal(clonedCfg)
	if err != nil {
		return fmt.Errorf("unable to marshal configuration: %w", err)
	}

	buf := new(bytes.Buffer)
	if err := json.Indent(buf, fileBytes, "", "  "); err != nil {
		return fmt.Errorf("unable to indent marshalled json: %w", err)
	}

	err = c.fileWriter(c.Path, buf.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("unable to write configuration file %q: %w", c.Path, err)
	}

	log.CtxLogger(ctx).Info("Successfully Updated configuration.json")
	return nil
}

// IsConfigModified returns true if any of the configuration files are modified.
func (c *Configure) IsConfigModified() bool {
	return c.OracleConfigModified || c.SQLServerConfigModified || c.RedisConfigModified || c.MySQLConfigModified
}
