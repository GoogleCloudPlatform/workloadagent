module github.com/GoogleCloudPlatform/workloadagent

go 1.25.0

replace github.com/GoogleCloudPlatform/workloadagent/internal => ./internal

replace github.com/GoogleCloudPlatform/workloadagent/protos => ./protos

require (
  cloud.google.com/go/monitoring v1.24.3
  github.com/DATA-DOG/go-sqlmock v1.5.0
  // Get the version by running:
  // go list -m -json github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries@main
  github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries v0.0.0-20260204142957-14e5a69745b7
  // Get the version by running:
  // go list -m -json github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos@main
  github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos v0.0.0-20260129072723-d3f35daa4026
  github.com/StackExchange/wmi v1.2.1
  github.com/cenkalti/backoff/v4 v4.3.0
  github.com/gammazero/workerpool v1.1.3
  github.com/go-sql-driver/mysql v1.8.1
  github.com/google/go-cmp v0.7.0
  github.com/jonboulle/clockwork v0.5.0
  github.com/kardianos/service v1.2.2
  github.com/lib/pq v1.10.9
  github.com/mattn/go-sqlite3 v1.14.16
  github.com/microsoft/go-mssqldb v1.4.0
  github.com/redis/go-redis/v9 v9.7.0
  github.com/sethvargo/go-retry v0.3.0
  github.com/shirou/gopsutil/v3 v3.24.5
  github.com/sijms/go-ora v1.3.2
  github.com/spf13/cobra v1.10.0
  github.com/spf13/pflag v1.0.9
  github.com/zieckey/goini v0.0.0-20240615065340-08ee21c836fb // indirect
  go.mongodb.org/mongo-driver/v2 v2.0.0
  go.uber.org/zap v1.27.0
  golang.org/x/crypto v0.47.0
  golang.org/x/exp v0.0.0-20240719175910-8a7402abbf56
  google.golang.org/api v0.265.0
  google.golang.org/genproto v0.0.0-20251202230838-ff82c1b0f217
  google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217
  google.golang.org/protobuf v1.36.11
)

require (
  cloud.google.com/go/artifactregistry v1.17.2
  github.com/GoogleCloudPlatform/agentcommunication_client v0.0.0-20250227185639-b70667e4a927
  github.com/golang/protobuf v1.5.4
  github.com/googleapis/gax-go v1.0.3
  google.golang.org/genproto/googleapis/rpc v0.0.0-20260128011058-8636f8732409
  k8s.io/api v0.35.0
  k8s.io/apiextensions-apiserver v0.35.0
  k8s.io/apimachinery v0.35.0
  k8s.io/client-go v0.35.0
)

require (
  cel.dev/expr v0.24.0 // indirect
  cloud.google.com/go v0.121.6 // indirect
  cloud.google.com/go/auth v0.18.1 // indirect
  cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
  cloud.google.com/go/compute/metadata v0.9.0 // indirect
  cloud.google.com/go/iam v1.5.3 // indirect
  cloud.google.com/go/logging v1.13.1 // indirect
  cloud.google.com/go/longrunning v0.7.0 // indirect
  cloud.google.com/go/secretmanager v1.16.0 // indirect
  cloud.google.com/go/storage v1.56.0 // indirect
  filippo.io/edwards25519 v1.1.0 // indirect
  github.com/BurntSushi/toml v0.3.1 // indirect
  github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.30.0 // indirect
  github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.53.0 // indirect
  github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.53.0 // indirect
  github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
  github.com/cespare/xxhash/v2 v2.3.0 // indirect
  github.com/cncf/xds/go v0.0.0-20251022180443-0feb69152e9f // indirect
  github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
  github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
  github.com/emicklei/go-restful/v3 v3.12.2 // indirect
  github.com/envoyproxy/go-control-plane/envoy v1.35.0 // indirect
  github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
  github.com/fatih/color v1.18.0 // indirect
  github.com/felixge/httpsnoop v1.0.4 // indirect
  github.com/fxamacker/cbor/v2 v2.9.0 // indirect
  github.com/gammazero/deque v0.2.0 // indirect
  github.com/go-jose/go-jose/v4 v4.1.3 // indirect
  github.com/go-logr/logr v1.4.3 // indirect
  github.com/go-logr/stdr v1.2.2 // indirect
  github.com/go-ole/go-ole v1.2.6 // indirect
  github.com/go-openapi/jsonpointer v0.21.0 // indirect
  github.com/go-openapi/jsonreference v0.20.2 // indirect
  github.com/go-openapi/swag v0.23.0 // indirect
  github.com/go-yaml/yaml v2.1.0+incompatible // indirect
  github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
  github.com/golang-sql/sqlexp v0.1.0 // indirect
  github.com/golang/snappy v0.0.4 // indirect
  github.com/google/gnostic-models v0.7.0 // indirect
  github.com/google/s2a-go v0.1.9 // indirect
  github.com/google/uuid v1.6.0 // indirect
  github.com/googleapis/enterprise-certificate-proxy v0.3.11 // indirect
  github.com/googleapis/gax-go/v2 v2.16.0 // indirect
  github.com/inconshreveable/mousetrap v1.1.0 // indirect
  github.com/josharian/intern v1.0.0 // indirect
  github.com/json-iterator/go v1.1.12 // indirect
  github.com/klauspost/compress v1.16.7 // indirect
  github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
  github.com/mailru/easyjson v0.7.7 // indirect
  github.com/mattn/go-colorable v0.1.13 // indirect
  github.com/mattn/go-isatty v0.0.20 // indirect
  github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
  github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
  github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
  github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
  github.com/pkg/errors v0.9.1 // indirect
  github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
  github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
  github.com/shoenig/go-m1cpu v0.1.6 // indirect
  github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
  github.com/tklauser/go-sysconf v0.3.12 // indirect
  github.com/tklauser/numcpus v0.6.1 // indirect
  github.com/x448/float16 v0.8.4 // indirect
  github.com/xdg-go/pbkdf2 v1.0.0 // indirect
  github.com/xdg-go/scram v1.1.2 // indirect
  github.com/xdg-go/stringprep v1.0.4 // indirect
  github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
  github.com/yusufpapurcu/wmi v1.2.4 // indirect
  go.opentelemetry.io/auto/sdk v1.2.1 // indirect
  go.opentelemetry.io/contrib/detectors/gcp v1.38.0 // indirect
  go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.61.0 // indirect
  go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.61.0 // indirect
  go.opentelemetry.io/otel v1.39.0 // indirect
  go.opentelemetry.io/otel/metric v1.39.0 // indirect
  go.opentelemetry.io/otel/sdk v1.39.0 // indirect
  go.opentelemetry.io/otel/sdk/metric v1.39.0 // indirect
  go.opentelemetry.io/otel/trace v1.39.0 // indirect
  go.uber.org/multierr v1.11.0 // indirect
  go.yaml.in/yaml/v2 v2.4.3 // indirect
  go.yaml.in/yaml/v3 v3.0.4 // indirect
  golang.org/x/lint v0.0.0-20181026193005-c67002cb31c3 // indirect
  golang.org/x/mod v0.31.0 // indirect
  golang.org/x/net v0.49.0 // indirect
  golang.org/x/oauth2 v0.34.0 // indirect
  golang.org/x/sync v0.19.0 // indirect
  golang.org/x/sys v0.40.0 // indirect
  golang.org/x/telemetry v0.0.0-20251203150158-8fff8a5912fc // indirect
  golang.org/x/term v0.39.0 // indirect
  golang.org/x/text v0.33.0 // indirect
  golang.org/x/time v0.14.0 // indirect
  golang.org/x/tools v0.40.0 // indirect
  google.golang.org/grpc v1.78.0 // indirect
  gopkg.in/evanphx/json-patch.v4 v4.13.0 // indirect
  gopkg.in/inf.v0 v0.9.1 // indirect
  gopkg.in/yaml.v3 v3.0.1 // indirect
  honnef.co/go/tools v0.0.0-20190102054323-c2f93a96b099 // indirect
  k8s.io/klog/v2 v2.130.1 // indirect
  k8s.io/kube-openapi v0.0.0-20250910181357-589584f1c912 // indirect
  k8s.io/utils v0.0.0-20251002143259-bc988d571ff4 // indirect
  sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
  sigs.k8s.io/randfill v1.0.0 // indirect
  sigs.k8s.io/structured-merge-diff/v6 v6.3.0 // indirect
  sigs.k8s.io/yaml v1.6.0 // indirect
)
