# Distributed Cache System

A production-ready distributed caching system built from scratch in 21 days using Go. This project implements a high-performance, fault-tolerant key-value store with advanced features like consistent hashing, LRU eviction, TTL, node failure detection, and real-world application (API rate limiter).

## Overview
- **Duration**: March 4, 2025 - March 24, 2025
- **Goal**: Develop a Minimum Viable Product (MVP) with production-grade features for campus recruitment resumes targeting top-tier companies.
- **Tech Stack**: Go, HTTP, TLS, Docker, YAML, expvar, pprof

## Features
- **Key-Value Storage**: Basic `/set`, `/get`, `/delete` operations via HTTP API.
- **High Concurrency**: Thread-safe with `sync.Mutex`, `Goroutines`, and `Channels` for parallel request handling.
- **Consistent Hashing**: Distributes data across nodes with virtual nodes for load balancing.
- **LRU Eviction**: Implements Least Recently Used (LRU) policy to manage memory efficiently.
- **TTL Support**: Automatically expires keys using time-based cleanup goroutines.
- **Fault Tolerance**: Heartbeat mechanism detects node failures and migrates data to healthy nodes.
- **Dynamic Nodes**: Supports adding/removing nodes with minimal disruption and data migration.
- **Security**: TLS-encrypted node communication and API key authentication.
- **Observability**: Metrics (hit rate, latency) exposed via `expvar` and performance profiling with `pprof`.
- **Real-World Application**: API rate limiter using the cache to restrict requests per IP.
- **Batch Operations**: `/mget` and `/mset` for efficient multi-key operations.
- **Testing**: Unit tests, HTTP tests, and chaos testing for reliability.
- **Deployment**: Dockerized with graceful shutdown support.