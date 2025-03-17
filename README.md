# Distributed Cache System

A production-ready distributed caching system built from scratch in Go, designed to handle high concurrency, scalability, and fault tolerance. This project implements a key-value store with advanced features such as consistent hashing, LRU eviction, TTL, node failure detection, and observability, making it suitable for real-world applications like API rate limiting.

Developed over 21 days (March 4–24, 2025) as part of a Minimum Viable Product (MVP) with additional high-level features, this project showcases strong engineering practices and problem-solving skills for campus recruitment at top-tier companies.

## Project Goals

- Build a production-grade distributed caching system with an MVP and advanced features.
- Core functionalities:
  - **Key-Value Storage**: Basic `/set`, `/get`, and `/delete` operations via HTTP.
  - **High Concurrency**: Thread-safe operations using Goroutines and Channels.
  - **Consistent Hashing**: Even data distribution across nodes.
  - **HTTP Interface**: RESTful API with JSON responses.
  - **LRU Eviction**: Memory management with Least Recently Used policy.
  - **Fault Tolerance**: Node failure detection via heartbeat mechanism.
  - **Observability**: Metrics like hit rate and latency.
  - **Security**: API key authentication and TLS encryption.
- Highlights:
  - Concurrent-safe design.
  - Dynamic node management.
  - TTL (Time-To-Live) for cache entries.
  - Monitoring and performance optimization.
  - Practical application: API rate limiter.

  ## Architecture

The system follows a modular, distributed design with the following components:

- **Single Node Cache**: An in-memory key-value store with LRU eviction and TTL support, implemented using a `map` and doubly-linked list (`container/list`).
- **Consistent Hash Ring**: Distributes keys across nodes using the FNV-1a hash algorithm, enhanced with virtual nodes for load balancing.
- **HTTP Server**: Handles client requests (`/set`, `/get`, `/delete`, `/batch_get`, `/batch_set`) and forwards them to appropriate nodes using consistent hashing.
- **Node Communication**: Nodes communicate via TLS-encrypted HTTP requests, with data synchronization (`/sync`) and health checks (`/health`).
- **Heartbeat Mechanism**: Detects node failures and triggers data migration to maintain availability.
- **Rate Limiter**: An example application using the cache to enforce API rate limits with a fixed-window counter.

### Directory Structure

cache-system/
├── cmd/server/         # Entry point (main.go)
├── config/            # Configuration files (config.yaml, config.go)
├── pkg/cache/         # Core cache logic (cache.go, consistent.go, heartbeat.go, metrics.go)
├── internal/api/      # API handlers and types (handler.go, types.go)
├── internal/util/     # Utilities (logger.go, httpclient.go)
├── logs/              # Log files
├── go.mod             # Go module file
└── README.md          # Project documentation