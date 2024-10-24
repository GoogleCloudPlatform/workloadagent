/*
Copyright 2024 Google LLC

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

package onetime

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	winsvc "github.com/kardianos/service"
)

var winlogger winsvc.Logger

// Winservice has args for winservice subcommands.
type Winservice struct {
	Service winsvc.Service
	Daemon  *cobra.Command
	// NOTE: Context is needed because kardianos/service for windows does not pass the context
	// to the service.Start(), service.Stop(), and service .Run() methods.
	ctx context.Context
}

// Start implements the subcommand interface for winservice.
func (w *Winservice) Start(s winsvc.Service) error {
	winlogger.Info("Winservice Start - starting the workload agent service")
	go w.run()
	return nil
}

// Stop implements the subcommand interface for winservice.
func (w *Winservice) Stop(s winsvc.Service) error {
	winlogger.Info("Winservice Run - stopping the workload agent service")
	return nil
}

func (w *Winservice) run() {
	winlogger.Info("Winservice Run - executing the daemon for the workload agent service")
	w.Daemon.Execute()
	winlogger.Info("Winservice Run - daemon execution is complete for the workload agent service")
}

// NewWinServiceCommand returns a new winservice command.
func NewWinServiceCommand(ctx context.Context, daemon *cobra.Command) *cobra.Command {
	w := &Winservice{
		Daemon: daemon,
		ctx:    ctx,
	}
	wsCmd := &cobra.Command{
		Use:   "winservice",
		Short: "operations for windows service operations",
		RunE:  w.Execute,
	}
	wsCmd.SetContext(ctx)
	return wsCmd
}

// Execute implements the subcommand interface for winservice.
func (w *Winservice) Execute(cmd *cobra.Command, args []string) error {
	fmt.Println("Winservice Execute - starting the workload agent service")
	config := &winsvc.Config{
		Name:        "google-cloud-workload-agent",
		DisplayName: "Google Cloud Workload Agent",
		Description: "Google Cloud Workload Agent",
	}
	s, err := winsvc.New(w, config)
	if err != nil {
		return fmt.Errorf("Winservice Execute - error creating Windows service manager interface for the workload agent service: %s", err)
	}
	w.Service = s

	winlogger, err = s.Logger(nil)
	if err != nil {
		return fmt.Errorf("Winservice Execute - error creating Windows Event Logger for the workload agent service: %s", err)
	}
	fmt.Println("Winservice Execute - Starting the workload agent service")
	winlogger.Info("Winservice Execute - Starting the workload agent service")

	err = s.Run()
	if err != nil {
		winlogger.Error(err)
	}
	winlogger.Info("Winservice Execute - The workload agent service is shutting down")
	return nil
}
