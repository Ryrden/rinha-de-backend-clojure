# Rinha de Back-end 2025

> ⚠️ **Disclaimer**: This code is optimized for competition performance and should not be used as-is in production environments. Some practices prioritize speed over traditional production best practices.

High-performance payment processing system built for the Rinha de Back-end competition by @zanfranceschi.

Competition repository [here](https://github.com/zanfranceschi/rinha-de-backend-2025).

## Tech Stack

- **Clojure 1.12.1** - Functional programming language
- **HTTP-kit** - High-performance HTTP server
- **Reitit** - Data-driven routing library
- **Redis 7** - In-memory data store for queuing and caching
- **Nginx** - Load balancer and reverse proxy

### System Features

- **Circuit Breaker Pattern** - Automatic failure detection and recovery
- **Health Monitoring** - Real-time processor health checks
- **Async Workers** - Background payment processing
- **Load Balancing** - Multiple API instances with Nginx
- **JVM Tuning** - Optimized for low-latency performance

## How to Run

T.B.D

The API will be available at `http://localhost:9999`

## API Endpoints

### Create Payment

`POST /payments`

```json
{
    "correlationId": "550e8400-e29b-41d4-a716-446655440000",
    "amount": 1000.50
}
```

**Response:**

```json
HTTP 202 Accepted
{
    "message": "Payment processed successfully"
}
```

### Health Check

`GET /`

```json
{
    "message": "Hello, World!",
    "status": "success",
    "timestamp": 1703123456789
}
```

### Payments Summary

`GET /payments-summary?from=2024-01-01T00:00:00Z&to=2024-01-31T23:59:59Z`

```json
{
    "default": {
        "totalRequests": 1500,
        "totalAmount": 75000.00
    },
    "fallback": {
        "totalRequests": 300,
        "totalAmount": 15000.00
    }
}
```

## System Architecture

```mermaid
flowchart TB
    subgraph "Load Balancer"
        LB[Nginx<br/>Port 9999]
    end
    
    subgraph "Application Instances"
        API1[API Instance 1<br/>Port 8080]
        API2[API Instance 2<br/>Port 8081]
    end
    
    subgraph "Data Layer"
        REDIS[(Redis<br/>Queue + Cache + Storage)]
    end
    
    subgraph "Processing Layer"
        WORKERS@{ shape: processes, label: "Payment Workers" }
        MONITOR[Health Monitor<br/>+ Circuit Breaker]
    end
    
    subgraph "External Payment Processors"
        DEFAULT[Default Processor<br/>Primary]
        FALLBACK[Fallback Processor<br/>Backup]
    end
    
    %% Request Flow
    LB -->|Round Robin| API1
    LB -->|Round Robin| API2
    
    %% Data Operations
    API1 -->|Enqueue Payments<br/>Get Summaries| REDIS
    API2 -->|Enqueue Payments<br/>Get Summaries| REDIS
    
    %% Worker Processing
    REDIS -->|Dequeue Payments| WORKERS
    WORKERS -->|Get Best Processor| MONITOR
    
    %% External Calls
    WORKERS -->|Send Payments| DEFAULT
    WORKERS -.->|Fallback| FALLBACK
    
    %% Health Checks
    MONITOR -.->|Health Checks| DEFAULT
    MONITOR -.->|Health Checks| FALLBACK
    
    %% Result Storage
    WORKERS -->|Save Results| REDIS
    
    %% Styling
    style LB fill:#e3f2fd
    style REDIS fill:#ffebee
    style WORKERS fill:#e8f5e8
    style MONITOR fill:#f3e5f5
    style DEFAULT fill:#e8f5e8
    style FALLBACK fill:#fff3e0
```

## Payment Processing Flow

### Main Request Flow

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Service
    participant Redis
    participant Worker
    participant Processor
    
    Client->>+API: POST /payments
    API->>+Service: Validate & process
    Service->>Service: Validate UUID & amount
    Service->>+Redis: Enqueue payment
    Redis-->>-Service: Queued
    Service-->>-API: 202 Accepted
    API-->>-Client: Payment accepted
    
    Note over Worker: Async Processing
    Worker->>+Redis: Dequeue payment
    Redis-->>-Worker: Payment data
    Worker->>+Processor: Send payment
    Processor-->>-Worker: Response
    Worker->>Redis: Save result
```

### Circuit Breaker & Health Monitoring

```mermaid
sequenceDiagram
    participant Worker
    participant Monitor
    participant CB as Circuit Breaker
    participant Default
    participant Fallback
    
    Worker->>+Monitor: Get best processor
    Monitor->>Monitor: Check health metrics
    Monitor-->>-Worker: Processor choice
    
    alt Both Processors Healthy
        Worker->>Default: Try default first
        Default-->>Worker: Success/Failure
    else Default Unhealthy
        Worker->>Fallback: Use fallback
        Fallback-->>Worker: Success/Failure
    else Both Failing
        Worker->>CB: Activate circuit breaker
        Note over CB: 5s protection window
        Worker->>Worker: Pause processing
    end
```

## Payment Summary Flow

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Service
    participant Redis
    
    Client->>+API: GET /payments-summary?from=...&to=...
    API->>+Service: Get payments summary
    Service->>Service: Parse date filters
    
    Note over Service,Redis: Parallel Lua Script Execution
    
    par Default Processor Data
        Service->>+Redis: EVAL Lua script<br/>ZRANGEBYSCORE payments:default
        Note over Redis: Lua Script:<br/>• Query sorted set by timestamp<br/>• Decode JSON payloads<br/>• Aggregate count & amount
        Redis-->>-Service: {totalRequests, totalAmount}
    and Fallback Processor Data  
        Service->>+Redis: EVAL Lua script<br/>ZRANGEBYSCORE payments:fallback
        Note over Redis: Same Lua aggregation<br/>for fallback processor
        Redis-->>-Service: {totalRequests, totalAmount}
    end
    
    Service->>Service: Combine results
    Service-->>-API: Summary response
    API-->>-Client: JSON summary
```
