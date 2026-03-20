# Show available recipes.
default:
    @just --list

# Start the development environment.
dev:
    docker compose -f docker-compose.dev.yml up -d

# Run all Go tests.
[group('test')]
test:
    go test ./...

[group('test')]
show-coverage:
    go tool cover -html=coverage.out
