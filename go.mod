module github.com/GoogleCloudPlatform/workloadagent

go 1.23

replace github.com/GoogleCloudPlatform/workloadagent/internal => ./internal

replace github.com/GoogleCloudPlatform/workloadagent/protos => ./protos

require (
  cloud.google.com/go/monitoring v1.17.1
  github.com/DATA-DOG/go-sqlmock v1.5.0
  github.com/GoogleCloudPlatform/workloadagentplatform/integration/common v0.0.0-20250130120719-3629ab2f4c43
  github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos v0.0.0-20250130133413-c271b7e1b5ad
  github.com/StackExchange/wmi v1.2.1
  github.com/cenkalti/backoff/v4 v4.1.3
  github.com/gammazero/workerpool v1.1.3
  github.com/go-sql-driver/mysql v1.8.1
  github.com/google/go-cmp v0.6.0
  github.com/jonboulle/clockwork v0.4.1-0.20230717050334-b1209715e43c
  github.com/kardianos/service v1.2.2
  github.com/mattn/go-sqlite3 v1.14.16
  github.com/microsoft/go-mssqldb v1.4.0
  github.com/redis/go-redis/v9 v9.7.0
  github.com/sethvargo/go-retry v0.3.0
  github.com/shirou/gopsutil/v3 v3.24.5
  github.com/sijms/go-ora v1.3.2
  github.com/spf13/cobra v1.8.1
  github.com/spf13/pflag v1.0.5
  go.uber.org/zap v1.27.0
  golang.org/x/crypto v0.31.0
  golang.org/x/exp v0.0.0-20230321023759-10a507213a29
  google.golang.org/api v0.168.0
  google.golang.org/genproto v0.0.0-20240205150955-31a09d347014
  google.golang.org/genproto/googleapis/api v0.0.0-20240205150955-31a09d347014
  google.golang.org/protobuf v1.36.4
)

require (
  cloud.google.com/go v0.112.0 // indirect
  cloud.google.com/go/compute v1.23.4 // indirect
  cloud.google.com/go/compute/metadata v0.2.3 // indirect
  cloud.google.com/go/iam v1.1.6 // indirect
  cloud.google.com/go/logging v1.9.0 // indirect
  cloud.google.com/go/longrunning v0.5.5 // indirect
  cloud.google.com/go/secretmanager v1.11.5 // indirect
  filippo.io/edwards25519 v1.1.0 // indirect
  github.com/cespare/xxhash/v2 v2.2.0 // indirect
  github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
  github.com/felixge/httpsnoop v1.0.4 // indirect
  github.com/gammazero/deque v0.2.0 // indirect
  github.com/go-logr/logr v1.4.1 // indirect
  github.com/go-logr/stdr v1.2.2 // indirect
  github.com/go-ole/go-ole v1.2.6 // indirect
  github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
  github.com/golang-sql/sqlexp v0.1.0 // indirect
  github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
  github.com/golang/protobuf v1.5.4 // indirect
  github.com/google/s2a-go v0.1.7 // indirect
  github.com/google/uuid v1.6.0 // indirect
  github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
  github.com/googleapis/gax-go/v2 v2.12.2 // indirect
  github.com/inconshreveable/mousetrap v1.1.0 // indirect
  github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
  github.com/natefinch/lumberjack v0.0.0-20230119042236-215739b3bcdc // indirect
  github.com/pkg/errors v0.9.1 // indirect
  github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
  github.com/shoenig/go-m1cpu v0.1.6 // indirect
  github.com/tklauser/go-sysconf v0.3.12 // indirect
  github.com/tklauser/numcpus v0.6.1 // indirect
  github.com/yusufpapurcu/wmi v1.2.4 // indirect
  go.opencensus.io v0.24.0 // indirect
  go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
  go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
  go.opentelemetry.io/otel v1.24.0 // indirect
  go.opentelemetry.io/otel/metric v1.24.0 // indirect
  go.opentelemetry.io/otel/trace v1.24.0 // indirect
  go.uber.org/multierr v1.10.0 // indirect
  golang.org/x/net v0.33.0 // indirect
  golang.org/x/oauth2 v0.17.0 // indirect
  golang.org/x/sync v0.10.0 // indirect
  golang.org/x/sys v0.28.0 // indirect
  golang.org/x/text v0.21.0 // indirect
  golang.org/x/time v0.5.0 // indirect
  google.golang.org/appengine v1.6.8 // indirect
  google.golang.org/genproto/googleapis/rpc v0.0.0-20240304161311-37d4d3c04a78 // indirect
  google.golang.org/grpc v1.62.0 // indirect
)
