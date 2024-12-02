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

# copy the service file to /lib/systemd/system/
cp /usr/share/google-cloud-workload-agent/service/google-cloud-workload-agent.service /lib/systemd/system/ &> /dev/null || true

# if the agent has an old configuration file then move it back into place
if [ -f /etc/google-cloud-workload-agent/configuration.json.bak ]; then
  mv /etc/google-cloud-workload-agent/configuration.json.bak /etc/google-cloud-workload-agent/configuration.json
fi

# enable the agent service and start it
systemctl enable google-cloud-workload-agent
systemctl start google-cloud-workload-agent

# log usage metrics for install
timeout 30 /usr/bin/google_cloud_workload_agent logusage -s INSTALLED &> /dev/null || true

# next steps instructions
echo ""
echo "##########################################################################"
echo "Google Cloud Workload Agent has been installed"
echo ""
echo "You can view the logs in /var/log/google-cloud-workload-agent.log"
echo ""
echo "Verify the agent is running with: "
echo  "    sudo systemctl status google-cloud-workload-agent"
echo "Configuration is available in /etc/google-cloud-workload-agent/configuration.json"
echo "##########################################################################"
echo ""
