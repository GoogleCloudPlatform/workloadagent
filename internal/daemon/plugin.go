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

package daemon

import (
	"context"
	"fmt"
	"net"

	"github.com/GoogleCloudPlatform/agentcommunication_client"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/encoding/prototext"
	pb "github.com/GoogleCloudPlatform/google-guest-agent/internal/plugin/proto/google_guest_agent/plugin"
	pbgrpc "github.com/GoogleCloudPlatform/google-guest-agent/internal/plugin/proto/google_guest_agent/plugin"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
)

const (
	// Label key for ACS messages to get the type of the contained message.
	messageTypeLabel = "message_type"

	// Status code for an unhealthy agent -non zero means failure to the Guest Agent.
	unhealthyStatusCode = 1
)

// Plugin is a subcommand that wraps the daemon subcommand.
// This subcommand will start listening for messages from the guest agent
// and when instructed to start plugin execution, it will start the daemon.
type Plugin struct {
	channelID    string
	endpoint     string
	address      string
	protocol     string
	errorlogfile string
	grpcServer   *grpc.Server
	daemon       *Daemon
	daemonCtx    context.Context
	daemonCancel context.CancelFunc
}

// NewPlugin creates a new Plugin with an underlying daemon instance to delegate to.
func NewPlugin(d *Daemon) *Plugin {
	return &Plugin{
		daemon:    d,
		daemonCtx: context.Background(),
	}
}

// PopulatePluginFlagValues uses the provided flags to set the plugin's primitive values.
func PopulatePluginFlagValues(p *Plugin, fs *pflag.FlagSet) {
	fs.StringVar(&p.channelID, "channel", "", "Channel ID on which to receive application specific messages")
	fs.StringVar(&p.endpoint, "endpoint", "", "Endpoint for the agent communication service")
	fs.StringVar(&p.protocol, "protocol", "", "TCP or UDP")
	fs.StringVar(&p.address, "address", "", "Address on which to listen for messages")
	fs.StringVar(&p.errorlogfile, "errorlogfile", "", "plugin error log file")
}

// NewPluginSubcommand creates a new plugin subcommand using the provided daemon as the worker.
func NewPluginSubcommand(p *Plugin) *cobra.Command {
	return &cobra.Command{
		Use:   "plugin",
		Short: "Plugin mode of the agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			return p.Execute(cmd.Context())
		},
	}
}

// Name returns the name of the plugin subcommand.
func (p *Plugin) Name() string {
	return "plugin"
}

// Synopsis returns a short string (less than one line) describing the plugin subcommand.
func (p *Plugin) Synopsis() string { return "Plugin Mode" }

// Usage returns a long string explaining the command and giving usage
// information.
func (p *Plugin) Usage() string {
	return "Executes the Workload Agent plugin when instructed to by the guest agent"
}

// Execute binds to an address and starts to listen for control messages
// from the guest agent as well as domain-specific messages from the Agent
// Communication Service. An error is returned if any connection cannot
// be initialized.
func (p *Plugin) Execute(ctx context.Context) error {
	log.Logger.Info("Starting plugin")
	if p.protocol == "" {
		return fmt.Errorf("protocol is required")
	}
	if p.address == "" {
		return fmt.Errorf("address is required")
	}
	listener, err := net.Listen(p.protocol, p.address)
	if err != nil {
		return fmt.Errorf("Failed to start listening on %q using %q: %v", p.address, p.protocol, err)
	}
	defer listener.Close()

	// This is used to receive control messages from the Guest Agent.
	server := grpc.NewServer()
	defer server.GracefulStop()

	// Enable the Guest Agent to handle the starting and stopping of the agent execution logic.
	pbgrpc.RegisterGuestAgentPluginServer(server, p)

	// Enable listening for domain-specific messages from the Agent Communication Service.
	var opts []option.ClientOption
	if p.endpoint != "" {
		opts = append(opts, option.WithEndpoint(p.endpoint))
	}
	conn, err := client.CreateConnection(ctx, p.channelID, false, opts...)
	if err != nil {
		return fmt.Errorf("Failed to create ACS connection: %v", err)
	}
	defer conn.Close()

	go func() {
		for {
			msg, err := conn.Receive()
			if err != nil {
				log.Logger.Fatalf("Failed to receive message from ACS: %v", err)
			}
			// The client will set the message type in the "message_type" label, we can key off that.
			messageType, ok := msg.GetLabels()[messageTypeLabel]
			if !ok {
				log.Logger.Warnf("Received message without the %q label: %v", messageType, prototext.Format(msg))
				continue
			}
			switch messageType {
			case "your custom message type here":
				go func() {
					// Unmarshall msg into your proto here and act accordingly. This must be done
					// asynchronously so the connection is not affected.
				}()
			default:
				log.Logger.Warnf("Unknown message type: %v", messageType)
			}
		}
	}()

	if err = server.Serve(listener); err != nil {
		return fmt.Errorf("Failed to listen for GRPC messages: %v", err)
	}

	return nil
}

// Apply applies the config sent or performs the work defined in the message.
// ApplyRequest is opaque to the agent and is expected to be well known contract
// between Plugin and the server itself. For e.g. service might want to update
// plugin config to enable/disable feature here plugins can react to such requests.
func (p *Plugin) Apply(ctx context.Context, msg *pb.ApplyRequest) (*pb.ApplyResponse, error) {
	return &pb.ApplyResponse{}, nil
}

// Start starts the plugin and initiates the plugin functionality.
// Until plugin receives Start request plugin is expected to be not functioning
// and just listening on the address handed off waiting for the request.
func (p *Plugin) Start(ctx context.Context, msg *pb.StartRequest) (*pb.StartResponse, error) {
	// [ctx] from request will be scoped to that of the request, when the request
	// is finished, the context is cancelled.
	// Treat this as the entry point for a plugin and create its own context.
	if p.daemonCancel != nil {
		log.Logger.Warn("Start called multiple times")
		return &pb.StartResponse{}, nil
	}
	p.daemonCtx, p.daemonCancel = context.WithCancel(context.Background())
	err := p.daemon.Execute(p.daemonCtx)
	return &pb.StartResponse{}, err
}

// Stop is the stop hook and implements any cleanup if required.
// Stop may be called if plugin revision is being changed.
// For e.g. if plugins want to stop some task it was performing or remove some
// state before exiting it can be done on this request.
func (p *Plugin) Stop(ctx context.Context, msg *pb.StopRequest) (*pb.StopResponse, error) {
	if p.daemonCancel == nil {
		log.Logger.Warn("Stop called before Start")
		return &pb.StopResponse{}, nil
	}
	err := p.daemonCtx.Err()
	if err != nil {
		return &pb.StopResponse{}, err
	}
	p.daemonCancel()
	p.daemonCancel = nil
	p.daemonCtx = context.Background()
	return &pb.StopResponse{}, nil
}

// GetStatus is the health check agent would perform to make sure plugin process
// is alive. If request fails process is considered dead and relaunched. Plugins
// can share any additional information to report it to the service. For e.g. if
// plugins detect some non-fatal errors causing it unable to offer some features
// it can reported in status which is sent back to the service by agent.
func (p *Plugin) GetStatus(ctx context.Context, msg *pb.GetStatusRequest) (*pb.Status, error) {
	if err := p.daemonCtx.Err(); err != nil {
		return &pb.Status{Code: unhealthyStatusCode, Results: []string{err.Error()}}, nil
	}
	return &pb.Status{Code: 0, Results: []string{"Plugin is running ok"}}, nil
}
