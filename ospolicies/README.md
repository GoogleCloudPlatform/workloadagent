# Google Cloud Agent for Compute Workloads OS Policies

These policies can be used with the
[Google Cloud VM Manager OS Configuration Management](https://cloud.google.com/compute/docs/os-configuration-management)
to automatically install the Agent for Compute Workloads on Google Cloud
instances.

## google-cloud-workload-agent-policy.yaml

This policy can be used with the Google Cloud Console OS Policy create UI to
install and update the Google Cloud Agent for Compute Workloads on Google Cloud
instances.

More information on creating an
[OS Policy from the Cloud Console](https://cloud.google.com/compute/vm-manager/docs/os-policies/create-os-policy-assignment)

## google-cloud-workload-agent-policy-gcloud.yaml

This policy can be used with the gcloud command line to automatically
install and update the Google Cloud Agent for Compute Workloads on Google Cloud
instances.

Example installation:

```
gcloud compute os-config os-policy-assignments create OS_POLICY_ASSIGNMENT_ID \
   --project=PROJECT \
   --location=ZONE \
   --file=google-cloud-workload-agent-policy.yaml \
   --async
```

More information on creating an
[OS Policy with gcloud](https://cloud.google.com/compute/vm-manager/docs/os-policies/create-os-policy-assignment#gcloud)

## License and Copyright

Copyright 2025 Google LLC.

Apache License, Version 2.0
