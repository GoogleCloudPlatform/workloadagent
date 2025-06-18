FROM debian:bookworm-slim

WORKDIR /

# https://github.com/debuerreotype/docker-debian-artifacts/issues/15
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates jq

RUN update-ca-certificates

COPY google_cloud_workload_agent openshift-docker-entrypoint.sh ./
RUN chmod +x openshift-docker-entrypoint.sh
COPY default_openshift_configuration.json /etc/google-cloud-workload-agent/configuration.json
# Redirect workload agent logs to stdout so that they are visible on the host.
RUN ln -sf /dev/stdout /var/log/google-cloud-workload-agent.log

ENTRYPOINT ["/openshift-docker-entrypoint.sh"]
