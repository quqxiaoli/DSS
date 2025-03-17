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