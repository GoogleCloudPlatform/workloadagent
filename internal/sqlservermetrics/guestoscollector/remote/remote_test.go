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

package remote

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/crypto/ssh"
)

var (
	dummyKey, dummyKnownHost = func() (string, string) {
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return "", ""
		}

		publicKey, err := ssh.NewPublicKey(&privateKey.PublicKey)
		if err != nil {
			return "", ""
		}

		return string(pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
		})), knownhosts.Line([]string{net.JoinHostPort("127.0.0.1", "22")}, publicKey)
	}()
)

type mockClient struct {
	outputErr bool
	input     string
}

func (m *mockClient) NewSession() (*ssh.Session, error) {
	return &ssh.Session{Stdin: bytes.NewBufferString(m.input)}, nil
}

func (m *mockClient) User() string {
	return ""
}

func (m *mockClient) SessionID() []byte {
	return nil
}

func (m *mockClient) ClientVersion() []byte {
	return nil
}

func (m *mockClient) ServerVersion() []byte {
	return nil
}

func (m *mockClient) RemoteAddr() net.Addr {
	return nil
}

func (m *mockClient) LocalAddr() net.Addr {
	return nil
}

func (m *mockClient) SendRequest(name string, wantReply bool, payload []byte) (bool, []byte, error) {
	return false, nil, nil
}

func (m *mockClient) OpenChannel(name string, data []byte) (ssh.Channel, <-chan *ssh.Request, error) {
	return nil, nil, nil
}

func (m *mockClient) Close() error {
	return nil
}

func (m *mockClient) Wait() error {
	return nil
}

func newMockClient(outputErr bool) mockClient {
	return mockClient{outputErr: outputErr, input: "any input string"}
}

type mockRemote struct {
	runErr           bool
	createSessionErr bool
	runCount         int
	input            string
}

func newMockRemote(runErr bool, createSessionErr bool) *mockRemote {
	return &mockRemote{runErr: runErr, createSessionErr: createSessionErr, runCount: 0, input: "any input string"}
}

func (m *mockRemote) Run(cmd string, session SSHSessionInterface) (string, error) {
	if m.runErr {
		return "", errors.New("run error")
	}
	m.runCount++
	return fmt.Sprintf("success run count: %v", m.runCount), nil
}

func (m *mockRemote) CreateSession(string) (SSHSessionInterface, error) {
	if m.createSessionErr {
		return nil, errors.New("create session error")
	}
	return &mockSession{outputErr: false, input: m.input}, nil
}

func (m *mockRemote) CreateClient() error {
	return nil
}

func (m *mockRemote) SetupKeys(string) error { return nil }

func (m *mockRemote) Close() error { return nil }

type mockSession struct {
	outputErr bool
	input     string
}

func (m *mockSession) Close() error { return nil }

func (m *mockSession) Output(cmd string) ([]byte, error) {
	if m.outputErr {
		return []byte(""), errors.New("output error")
	}
	return []byte("output"), nil
}

func TestPrivateKey(t *testing.T) {
	testcases := []struct {
		name         string
		keyFileExist bool
		keyFileValid bool
		wantErr      bool
	}{
		{
			name:         "success",
			keyFileExist: true,
			keyFileValid: true,
		},
		{
			name:    "failure with reading key file error",
			wantErr: true,
		},
		{
			name:         "failure with parsing key error",
			keyFileExist: true,
			wantErr:      true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tmpKeyPath := t.TempDir() + "/key"
			if tc.keyFileExist {
				if err := os.WriteFile(tmpKeyPath, []byte(`any content`), 0666); err != nil {
					t.Fatalf("Failed to write file: %v", err)
				}
				if tc.keyFileValid {
					if err := os.WriteFile(tmpKeyPath, []byte(dummyKey), 0666); err != nil {
						t.Fatalf("Failed to write file: %v", err)
					}
				}
			}
			r := &remote{
				key: &key{},
			}
			got := r.privateKey(tmpKeyPath)
			if gotError := got != nil; gotError != tc.wantErr {
				t.Errorf("privateKey(%q) = %v, wantError: %v", tmpKeyPath, got, tc.wantErr)
			}
		})
	}
}

func TestSetupKeys(t *testing.T) {
	tests := []struct {
		name            string
		privateKeyError bool
		publicKeyError  bool
		wantErr         bool
	}{
		{
			name:            "privateKey() returned error",
			privateKeyError: true,
			publicKeyError:  true,
			wantErr:         true,
		},
		{
			name:            "privateKey() succeeded, publicKey() failed",
			privateKeyError: false,
			publicKeyError:  true,
			wantErr:         true,
		},
		{
			name:            "public and private key succeeded",
			privateKeyError: false,
			publicKeyError:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			tmpKnownHostPath := tempDir + "/known_hosts"
			r := &remote{
				ip: "127.0.0.1",
				key: &key{
					knownHostsPath: tmpKnownHostPath,
				},
			}
			tmpKeyPath := tempDir + "/privatekey"
			if !tc.privateKeyError {
				if err := os.WriteFile(tmpKeyPath, []byte(dummyKey), 0666); err != nil {
					t.Fatalf("Failed to write file: %v", err)
				}
			}

			if !tc.publicKeyError {
				if err := os.WriteFile(tmpKnownHostPath, []byte(dummyKnownHost), 0666); err != nil {
					t.Fatalf("Failed to write file: %v", err)
				}
			}

			got := r.SetupKeys(tmpKeyPath)
			if gotErr := got != nil; gotErr != tc.wantErr {
				t.Errorf("SetupKeys()=%v, want error: %v", got, tc.wantErr)
			}
		})
	}
}

func TestCreateClient(t *testing.T) {
	testcases := []struct {
		name          string
		nilPrivateKey bool
		nilPublicKey  bool
		wantErr       bool
	}{
		{
			name:    "error while dailErr",
			wantErr: true,
		},
		{
			name:          "nil private key",
			nilPrivateKey: true,
			wantErr:       true,
		},
		{
			name:         "nil public key",
			nilPublicKey: true,
			wantErr:      true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			r := &remote{
				key: &key{},
			}
			if !tc.nilPrivateKey {
				tmpKeyPath := t.TempDir() + "/privatekey"
				if err := os.WriteFile(tmpKeyPath, []byte(dummyKey), 0666); err != nil {
					t.Fatalf("Failed to write file: %v", err)
				}
				r.privateKey(tmpKeyPath)
			}
			if !tc.nilPublicKey {
				tmpKeyPath := t.TempDir() + "/privatekey"
				if err := os.WriteFile(tmpKeyPath, []byte(dummyKey), 0666); err != nil {
					t.Fatalf("Failed to write file: %v", err)
				}
				r.privateKey(tmpKeyPath)
				r.key.PublicKey = r.key.PrivateKey.PublicKey()
				if tc.nilPrivateKey {
					r.key.PrivateKey = nil
				}
			}
			got := r.CreateClient()
			if gotError := got != nil; gotError != tc.wantErr {
				t.Errorf("CreateClient() = %v, wantError %v", got, tc.wantErr)
			}
		})
	}
}

func TestPublicKey(t *testing.T) {
	tmpKnownHostPath := t.TempDir() + "/knownhost"
	if err := os.WriteFile(tmpKnownHostPath, []byte(dummyKnownHost), 0666); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	testcases := []struct {
		name           string
		wantErr        bool
		ip             string
		knownHostsPath string
		publicKeyFound bool
	}{
		{
			name:           "success",
			wantErr:        false,
			ip:             "127.0.0.1",
			knownHostsPath: tmpKnownHostPath,
			publicKeyFound: true,
		},
		{
			name:           "unable to find ip in known host path",
			wantErr:        true,
			ip:             "127.0.0.6",
			knownHostsPath: tmpKnownHostPath,
			publicKeyFound: false,
		},
		{
			name:           "fail because unable to find known host path",
			wantErr:        true,
			ip:             "127.0.0.1",
			knownHostsPath: "",
		},
	}

	for _, tc := range testcases {

		t.Run(tc.name, func(t *testing.T) {
			r := &remote{ip: tc.ip, key: &key{knownHostsPath: tc.knownHostsPath}}
			got := r.publicKey(r.ip, r.key.knownHostsPath)
			if gotError := got != nil; gotError != tc.wantErr {
				t.Errorf("publicKey() = %v, wantError %v", got, tc.wantErr)
			}
			if !tc.publicKeyFound && r.key.PublicKey != nil {
				t.Errorf("publicKey() = %v, want nil", r.key.PublicKey)
			}
		})
	}
}

// checks CreateSession() returned nil correctly
func TestCreateSession(t *testing.T) {
	testcases := []struct {
		name             string
		mockedSSHClient  mockClient
		sessionOutputErr bool
		wantErr          bool
	}{
		{
			name:             "success",
			mockedSSHClient:  newMockClient(false),
			sessionOutputErr: false,
			wantErr:          false,
		},
		{
			name:             "failed to get session output",
			mockedSSHClient:  newMockClient(true),
			sessionOutputErr: true,
			wantErr:          true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			r1 := &remote{}
			if !tc.sessionOutputErr {
				r1.client = &tc.mockedSSHClient
			}
			got, err := r1.CreateSession("any input string")
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("CreateSession()=%v, wantError: %v", gotErr, tc.wantErr)
			}
			// if got has a valid value, it should not have an error
			if got != nil && tc.wantErr {
				t.Errorf("CreateSession()=%v, want: %v", got, nil)
			}
		})
	}
}

func TestRun(t *testing.T) {
	testcases := []struct {
		name             string
		want             string
		sessionOutputErr bool
		wantErr          bool
	}{
		{
			name:             "success",
			want:             "output",
			sessionOutputErr: false,
			wantErr:          false,
		},
		{
			name:             "failed to get session output",
			want:             "",
			sessionOutputErr: true,
			wantErr:          true,
		},
	}
	for _, tc := range testcases {
		s := &mockSession{
			outputErr: tc.sessionOutputErr,
		}

		r := &remote{}
		got, err := r.Run("any", s)
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("Run()=%v, wantError: %v", got, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("Run()=%v, want: %v", got, tc.want)
		}
	}
}

func TestRunCommandWithPipes(t *testing.T) {
	testcases := []struct {
		name             string
		cmd              string
		want             string
		runErr           bool
		createSessionErr bool
		wantErr          bool
	}{
		{
			name:             "create session error",
			want:             "",
			createSessionErr: true,
			wantErr:          true,
		},
		{
			name:             "run error",
			want:             "",
			runErr:           true,
			createSessionErr: false,
			wantErr:          true,
		},
		{
			name:             "success for 1 pipe",
			cmd:              "any",
			want:             "success run count: 1",
			runErr:           false,
			createSessionErr: false,
			wantErr:          false,
		},
		{
			name:             "success for 3 pipe",
			cmd:              "any1 | any2 | any3",
			want:             "success run count: 3",
			runErr:           false,
			createSessionErr: false,
			wantErr:          false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RunCommandWithPipes(tc.cmd, newMockRemote(tc.runErr, tc.createSessionErr))
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("RunCommandWithPipes()=%v, wantError: %v", got, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("RunCommandWithPipes()=%v, want: %v", got, tc.want)
			}
		})
	}
}
