# Development

Guide for developers who want to contribute to DataFlow Operator or set up a local development environment.

> **Note**: This is a simplified English version. For complete documentation, see the [Russian version](../ru/development.md).

## Prerequisites

- Go 1.21 or higher
- Docker and Docker Compose
- kubectl configured to work with the cluster
- Helm 3.0+ (for testing installation)
- Make (optional, for using Makefile)

## Environment Setup

### Cloning the Repository

```bash
git clone <repository-url>
cd dataflow
```

### Installing Dependencies

```bash
go mod download
go mod tidy
```

### Installing Development Tools

```bash
# Install controller-gen
make controller-gen

# Install envtest
make envtest
```

## Local Development

### Starting Dependencies

Start all necessary services via docker-compose:

```bash
docker-compose up -d
```

This will start:
- Kafka (port 9092) with Kafka UI (port 8080)
- PostgreSQL (port 5432) with pgAdmin (port 5050)

### Running Operator Locally

```bash
# Generate code and manifests
make generate
make manifests

# Install CRD in cluster (if using kind/minikube)
make install

# Run operator
make run
```

Or use the script:

```bash
./scripts/run-local.sh
```

## Project Structure

```
dataflow/
├── api/v1/                    # CRD definitions
│   ├── dataflow_types.go      # DataFlow resource types
│   └── groupversion_info.go    # API version
├── internal/
│   ├── connectors/            # Connectors for sources/sinks
│   ├── transformers/          # Message transformations
│   ├── processor/             # Message processor
│   ├── controller/            # Kubernetes controller
│   └── types/                 # Internal types
├── config/                    # Kubernetes configuration
│   ├── crd/                   # CRD manifests
│   ├── rbac/                  # RBAC manifests
│   └── samples/              # DataFlow resource examples
├── helm/                      # Helm Chart
├── docs/                      # MkDocs documentation
├── test/                      # Tests
└── scripts/                   # Helper scripts
```

## Code Generation

### Generate CRD and RBAC

```bash
make manifests
```

This command generates:
- CRD manifests in `config/crd/bases/`
- RBAC manifests in `config/rbac/`

### Generate DeepCopy Methods

```bash
make generate
```

Generates `DeepCopy` methods for all types in `api/v1/`.

## Testing

### Unit Tests

```bash
# Run all unit tests
make test-unit

# Run tests with coverage
make test

# Run tests for specific package
go test ./internal/connectors/... -v
```

### Integration Tests

```bash
# Set up kind cluster
./scripts/setup-kind.sh

# Run integration tests
make test-integration
```

## Building

### Local Build

```bash
# Build binary
make build

# Binary will be in bin/manager
./bin/manager
```

### Docker Image Build

```bash
# Build image
make docker-build IMG=your-registry/dataflow-operator:v1.0.0

# Push image
make docker-push IMG=your-registry/dataflow-operator:v1.0.0
```

## Adding a New Connector

1. Define types in API (`api/v1/dataflow_types.go`)
2. Implement connector (`internal/connectors/newconnector.go`)
3. Register in factory (`internal/connectors/factory.go`)
4. Generate code (`make generate && make manifests`)
5. Write tests

## Adding a New Transformation

1. Define types in API (`api/v1/dataflow_types.go`)
2. Implement transformation (`internal/transformers/newtransformation.go`)
3. Register in factory (`internal/transformers/factory.go`)
4. Generate and test (`make generate && make test`)

## Debugging

### Logging

Use structured logging:

```go
import "github.com/go-logr/logr"

logger.Info("Processing message", "messageId", msg.ID)
logger.Error(err, "Failed to process", "messageId", msg.ID)
```

### Debugging Controller

```bash
# Run with detailed logging
go run ./main.go --zap-log-level=debug
```

## Code Formatting and Linting

### Format Code

```bash
make fmt
```

### Check Code

```bash
make vet
```

## Contributing

### Development Process

1. Create an issue to discuss changes
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make changes and add tests
4. Ensure all tests pass: `make test`
5. Format code: `make fmt`
6. Create Pull Request

### Code Standards

- Follow Go code review comments
- Add comments for public functions
- Write tests for new functionality
- Update documentation as needed

For complete development guide, see the [Russian version](../ru/development.md).

