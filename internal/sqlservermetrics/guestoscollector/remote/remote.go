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

// Package remote ssh'es into remote machines and runs a command
package remote

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/crypto/ssh"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
)

// SSHClientInterface abstracts the client struct from ssh package
type SSHClientInterface interface {
	ssh.Conn

	NewSession() (*ssh.Session, error)
}

// SSHSessionInterface abstracts the session struct from ssh package
type SSHSessionInterface interface {
	Output(string) ([]byte, error)
	Close() error
}

// Executor interface for executing remote commands
type Executor interface {
	SetupKeys(string) error
	CreateClient() error
	CreateSession(string) (SSHSessionInterface, error)
	Run(string, SSHSessionInterface) (string, error)
	Close() error
}

// remote contains the key for remote ssh'ing
type remote struct {
	user   string
	ip     string
	port   int32
	key    *key
	client SSHClientInterface
}

type key struct {
	PrivateKey     ssh.Signer
	PublicKey      ssh.PublicKey
	knownHostsPath string
}

// NewRemote attempts to find connect to remote ssh server with private key
func NewRemote(ipaddr, user string, port int32) Executor {
	return &remote{
		ip:   ipaddr,
		port: port,
		user: user,
		key:  &key{},
	}
}

// SetupKeys load the key from given path and returns error if it failed to read the key file.
func (r *remote) SetupKeys(privateKeyPath string) error {
	if err := r.privateKey(privateKeyPath); err != nil {
		return err
	}
	knownHostsPath := filepath.Join(filepath.Dir(privateKeyPath), "known_hosts")
	if err := r.publicKey(r.ip, knownHostsPath); err != nil {
		return err
	}
	return nil
}

func (r *remote) privateKey(privateKeyPath string) error {
	privateKeyBytes, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return fmt.Errorf("an error occurred while reading the key file. %v", err)
	}

	privateKey, err := ssh.ParsePrivateKey(privateKeyBytes)
	if err != nil {
		return fmt.Errorf("an error occurred while parsing the private key. %v", err)
	}

	r.key.PrivateKey = privateKey
	return nil
}

// publicKey scans the known hosts file and gets a public key for the valid host that we are trying to ssh into
func (r *remote) publicKey(host, knownHostsPath string) error {
	// parse OpenSSH known_hosts file
	// ssh or use ssh-keyscan to get initial key
	fd, err := os.Open(knownHostsPath)
	if err != nil {
		return fmt.Errorf("an error occurred when opening known_hosts. %v", err)
	}
	defer fd.Close()

	// support -H parameter for ssh-keyscan
	hashhost := knownhosts.HashHostname(host)

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		_, hosts, key, _, _, err := ssh.ParseKnownHosts(scanner.Bytes())
		if err != nil {
			log.Logger.Errorf("failed to parse known_hosts: %s", scanner.Text())
			continue
		}

		for _, h := range hosts {
			if h == host || h == hashhost {
				r.key.PublicKey = key
				return nil
			}
		}
	}

	return fmt.Errorf("known host file does not contain host %s; please SSH into host first to verify fingerprint", host)
}

// CreateClient creates ssh client based on private key and public key from Remote struct.
func (r *remote) CreateClient() error {
	if r.key.PublicKey == nil {
		return fmt.Errorf("no public key found. please make sure SetupKeys() is called before calling CreateClient()")
	}
	if r.key.PrivateKey == nil {
		return fmt.Errorf("no private key found. please make sure SetupKeys() is called before calling CreateClient()")
	}
	c, err := ssh.Dial("tcp", net.JoinHostPort(r.ip, strconv.FormatInt(int64(r.port), 10)), &ssh.ClientConfig{
		User:            r.user,
		HostKeyCallback: ssh.FixedHostKey(r.key.PublicKey),
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(r.key.PrivateKey),
		},
	})
	if err != nil {
		return fmt.Errorf("an error occurred while ssh dialing. %v", err)
	}
	r.client = c
	return nil
}

// CreateSession creates ssh session.
func (r *remote) CreateSession(input string) (SSHSessionInterface, error) {
	if r.client == nil {
		return nil, fmt.Errorf("no client created. please make sure CreateClient() is called before calling CreateSession()")
	}
	session, err := r.client.NewSession()
	if err != nil {
		return nil, err
	}
	if input != "" {
		session.Stdin = bytes.NewBufferString(input)
	}
	return session, nil
}

func (r *remote) Close() error {
	return r.client.Close()
}

// Run runs a remote ssh command ex: output, err := remoteRun("root", "MY_IP", "privateKey", "22", "ls -l")
func (r *remote) Run(cmd string, session SSHSessionInterface) (string, error) {
	output, err := session.Output(cmd)
	if err != nil {
		return "", fmt.Errorf("An error occurred while running the cmd %v, %v", cmd, err)
	}
	return strings.TrimSuffix(string(output), "\n"), nil
}

// RunCommandWithPipes runs consecutive remote commands that have |
func RunCommandWithPipes(cmd string, e Executor) (string, error) {
	commands := strings.Split(cmd, "|")
	input := ""
	for _, command := range commands {
		f := func() error {
			s, err := e.CreateSession(input)
			if err != nil {
				return fmt.Errorf("Failed to create a session. %v", err)
			}
			defer s.Close()

			if input, err = e.Run(command, s); err != nil {
				return err
			}
			return nil
		}

		if err := f(); err != nil {
			return "", err
		}
	}
	// the last "input" from Run is the final return value
	return input, nil
}
