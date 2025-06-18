#!/bin/bash
# Copyright 2025 Google LLC
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

# https://stackoverflow.com/a/65396324
check_vars() {
    var_names=("$@")
    for var_name in "${var_names[@]}"; do
        [ -z "${!var_name}" ] && echo "$var_name is unset." && var_unset=true
    done
    [ -n "$var_unset" ] && exit 1
    return 0
}

# TODO: check for existence of secret files
# Sensitive variables are from mounted secrets
OCP_USERNAME=$(cat /etc/secrets/ocp-username)
OCP_PASSWORD=$(cat /etc/secrets/ocp-password)

# Non sensitive variables with reasonable defaults
LOG_LEVEL="${LOG_LEVEL:-INFO}"
COLLECTION_FREQUENCY="${COLLECTION_FREQUENCY:-60s}"

# Check that all required variables are set
check_vars OCP_USERNAME OCP_PASSWORD OCP_HOST PROJECT_ID REGION

# Generate the workload agentconfig file
CONFIG_FILE_PATH=/etc/google-cloud-workload-agent/configuration.json

# TODO: add option for configmap

# TODO: handle merging with default_openshift_configuration.json
# Generate the configuration.json file used by the workloadagent
# https://www.baeldung.com/linux/jq-json-nesting-variables
jq -nc \
  --arg log_level "$LOG_LEVEL" \
  --arg collection_frequency "$COLLECTION_FREQUENCY" \
  --arg ocp_username "$OCP_USERNAME" \
  --arg ocp_password "$OCP_PASSWORD" \
  --arg ocp_host "$OCP_HOST" \
  --arg project_id "$PROJECT_ID" \
  --arg region "$REGION" \
  '{
    log_level: $log_level,
    common_discovery: {
      collection_frequency: $collection_frequency
    },
    cloud_properties: {
      project_id: $project_id,
      region: $region
    },
    openshift_configuration: {
      enabled: true,
      connection_parameters: {
        username: $ocp_username,
        password: $ocp_password,
        host: $ocp_host,
      }
    },
    common_discovery: {
      enabled: false
    },
    sqlserver_configuration: {
      enabled: false
    },
    oracle_configuration: {
      enabled: false
    },
    mysql_configuration: {
      enabled: false
    },
    redis_configuration: {
      enabled: false
    },
    postgres_configuration: {
      enabled: false
    }
}' > "$CONFIG_FILE_PATH"

# Start the daemon
/google_cloud_workload_agent startdaemon
