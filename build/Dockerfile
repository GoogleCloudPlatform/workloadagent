FROM ghcr.io/jqlang/jq:latest as jq
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

LABEL name="workload-agent" \
      vendor="Google Cloud" \
      version="0.0.1" \
      release="1" \
      summary="Google Cloud Agent for Compute Workloads" \
      description="The Agent for Compute Workloads lets you monitor and evaluate workloads running on Compute Engine. Once you install the agent on the compute instances that host your workload, the agent collects metrics from your workload and instances, and sends them to Google Cloud services. Currently, the agent only sends to Google Cloud Workload Manager where you can the evaluation service of Workload Manager to evaluate your workload against industry best practices and those recommended by Google Cloud."
      maintainer="workload-agent-team@google.com"

WORKDIR /

RUN mkdir -p /licenses
COPY LICENSE /licenses/

COPY --from=jq /jq /usr/local/bin/jq
COPY google_cloud_workload_agent openshift-docker-entrypoint.sh ./

RUN mkdir -p /etc/google-cloud-workload-agent
COPY default_openshift_configuration.json /etc/google-cloud-workload-agent/configuration.json

# Run native UBI commands to setup logs
RUN touch /var/log/google-cloud-workload-agent.log && \
    ln -sf /dev/stdout /var/log/google-cloud-workload-agent.log && \
    chmod +x /openshift-docker-entrypoint.sh

# Set a non-root user
USER 1001

ENTRYPOINT ["/openshift-docker-entrypoint.sh"]
