module github.com/GoogleCloudPlatform/workloadagent

go 1.24

replace github.com/GoogleCloudPlatform/workloadagent/internal => ./internal

replace github.com/GoogleCloudPlatform/workloadagent/protos => ./protos

require (
  cloud.google.com/go/monitoring v1.23.0
  github.com/DATA-DOG/go-sqlmock v1.5.0
  // Get the version by running:
  // go list -m -json github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries@main
  github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries v0.0.0-20250708111514-b11e0b3dc2e3
  // Get the version by running:
  // go list -m -json github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos@main
  github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos v0.0.0-20250708111514-b11e0b3dc2e3
  github.com/StackExchange/wmi v1.2.1
  github.com/cenkalti/backoff/v4 v4.3.0
  github.com/gammazero/workerpool v1.1.3
  github.com/go-sql-driver/mysql v1.8.1
  github.com/google/go-cmp v0.6.0
  github.com/jonboulle/clockwork v0.5.0
  github.com/kardianos/service v1.2.2
  github.com/lib/pq v1.10.9
  github.com/mattn/go-sqlite3 v1.14.16
  github.com/microsoft/go-mssqldb v1.4.0
  github.com/redis/go-redis/v9 v9.7.0
  github.com/sethvargo/go-retry v0.3.0
  github.com/shirou/gopsutil/v3 v3.24.5
  github.com/sijms/go-ora v1.3.2
  github.com/spf13/cobra v1.8.1
  github.com/spf13/pflag v1.0.5
  github.com/zieckey/goini v0.0.0-20240615065340-08ee21c836fb // indirect
  go.mongodb.org/mongo-driver/v2 v2.0.0
  go.uber.org/zap v1.27.0
  golang.org/x/crypto v0.32.0
  golang.org/x/exp v0.0.0-20230321023759-10a507213a29
  google.golang.org/api v0.220.0
  google.golang.org/genproto v0.0.0-20250204164813-702378808489
  google.golang.org/genproto/googleapis/api v0.0.0-20250204164813-702378808489
  google.golang.org/protobuf v1.36.5
)

require (
  cloud.google.com/go/artifactregistry v1.16.1
  github.com/GoogleCloudPlatform/agentcommunication_client v0.0.0-20250227185639-b70667e4a927
  github.com/googleapis/gax-go v1.0.3
)

require (
  cloud.google.com/go v0.118.0 // indirect
  cloud.google.com/go/auth v0.14.1 // indirect
  cloud.google.com/go/auth/oauth2adapt v0.2.7 // indirect
  cloud.google.com/go/compute/metadata v0.6.0 // indirect
  cloud.google.com/go/iam v1.3.1 // indirect
  cloud.google.com/go/logging v1.13.0 // indirect
  cloud.google.com/go/longrunning v0.6.4 // indirect
  cloud.google.com/go/secretmanager v1.14.4 // indirect
  filippo.io/edwards25519 v1.1.0 // indirect
  github.com/BurntSushi/toml v0.3.1 // indirect
  github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
  github.com/cespare/xxhash/v2 v2.3.0 // indirect
  github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
  github.com/fatih/color v1.18.0 // indirect
  github.com/felixge/httpsnoop v1.0.4 // indirect
  github.com/gammazero/deque v0.2.0 // indirect
  github.com/go-logr/logr v1.4.2 // indirect
  github.com/go-logr/stdr v1.2.2 // indirect
  github.com/go-ole/go-ole v1.2.6 // indirect
  github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
  github.com/golang-sql/sqlexp v0.1.0 // indirect
  github.com/golang/protobuf v1.5.4 // indirect
  github.com/golang/snappy v0.0.4 // indirect
  github.com/google/s2a-go v0.1.9 // indirect
  github.com/google/uuid v1.6.0 // indirect
  github.com/googleapis/enterprise-certificate-proxy v0.3.4 // indirect
  github.com/googleapis/gax-go/v2 v2.14.1 // indirect
  github.com/inconshreveable/mousetrap v1.1.0 // indirect
  github.com/klauspost/compress v1.16.7 // indirect
  github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
  github.com/mattn/go-colorable v0.1.13 // indirect
  github.com/mattn/go-isatty v0.0.20 // indirect
  github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
  github.com/pkg/errors v0.9.1 // indirect
  github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
  github.com/shoenig/go-m1cpu v0.1.6 // indirect
  github.com/tklauser/go-sysconf v0.3.12 // indirect
  github.com/tklauser/numcpus v0.6.1 // indirect
  github.com/xdg-go/pbkdf2 v1.0.0 // indirect
  github.com/xdg-go/scram v1.1.2 // indirect
  github.com/xdg-go/stringprep v1.0.4 // indirect
  github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
  github.com/yusufpapurcu/wmi v1.2.4 // indirect
  go.opentelemetry.io/auto/sdk v1.1.0 // indirect
  go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.58.0 // indirect
  go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.58.0 // indirect
  go.opentelemetry.io/otel v1.34.0 // indirect
  go.opentelemetry.io/otel/metric v1.34.0 // indirect
  go.opentelemetry.io/otel/trace v1.34.0 // indirect
  go.uber.org/multierr v1.10.0 // indirect
  golang.org/x/lint v0.0.0-20181026193005-c67002cb31c3 // indirect
  golang.org/x/mod v0.17.0 // indirect
  golang.org/x/net v0.34.0 // indirect
  golang.org/x/oauth2 v0.27.0 // indirect
  golang.org/x/sync v0.10.0 // indirect
  golang.org/x/sys v0.29.0 // indirect
  golang.org/x/text v0.21.0 // indirect
  golang.org/x/time v0.9.0 // indirect
  golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
  google.golang.org/genproto/googleapis/rpc v0.0.0-20250127172529-29210b9bc287 // indirect
  google.golang.org/grpc v1.70.0 // indirect
  honnef.co/go/tools v0.0.0-20190102054323-c2f93a96b099 // indirect
)
