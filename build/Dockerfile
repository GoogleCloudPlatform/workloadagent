FROM ghcr.io/jqlang/jq:latest as jq

# Hack to create the log file so that we can redirect it later.
FROM busybox:latest as busybox
RUN touch /google-cloud-workload-agent.log

FROM us.gcr.io/gke-release/gke-distroless/bash
WORKDIR /
COPY --from=jq /jq /jq
COPY google_cloud_workload_agent openshift-docker-entrypoint.sh ./
COPY default_openshift_configuration.json /etc/google-cloud-workload-agent/configuration.json
# Redirect workload agent logs to stdout so that they are visible on the host.
COPY --from=busybox /google-cloud-workload-agent.log /var/log/google-cloud-workload-agent.log
RUN ["ln", "-sf", "/dev/stdout", "/var/log/google-cloud-workload-agent.log"]
ENTRYPOINT ["/openshift-docker-entrypoint.sh"]
