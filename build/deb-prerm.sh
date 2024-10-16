#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# disable the agent service and stop it
systemctl disable google-cloud-workload-agent
systemctl stop google-cloud-workload-agent

# remove the service file
rm -f /lib/systemd/system/google-cloud-workload-agent.service

# log usage metrics for uninstall
timeout 30 /usr/bin/google_cloud_workload_agent logusage -s UNINSTALLED &> /dev/null || true
