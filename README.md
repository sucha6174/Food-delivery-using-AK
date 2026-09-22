# FlashBite: Real-Time Food Delivery Tracking System with Apache Kafka

An event-driven, distributed order tracking and real-time analytics system built with **Apache Kafka**, **Python**, **Flask**, **Docker**, and a **Glassmorphic Live Web Dashboard**.

---

## Architecture Overview

The system models a food delivery platform where order lifecycle events are produced and consumed by decoupled microservices communicating through Apache Kafka.

```
 +-----------------------------------------------------------------------------------+
 |                                   PRODUCER                                        |
 |   producer.py (Order Simulator: PLACED -> CONFIRMED -> PREPARING -> O4D -> DLVD)  |
 +-----------------------------------------+-----------------------------------------+
                                           |
                                           | Publishes events (Key: order_id)
                                           v
 +-----------------------------------------------------------------------------------+
 |                             APACHE KAFKA BROKER (9092)                            |
 |                                Topic: order-events                                |
 |            +-------------------+-------------------+-------------------+          |
 |            |    Partition 0    |    Partition 1    |    Partition 2    |          |
 |            +-------------------+-------------------+-------------------+          |
 |                     |                                       |                     |
 +---------------------|---------------------------------------|---------------------+
                       |                                       |
                       | Consumes (Group: status-tracker)      | Consumes (Group: analytics)
                       v                                       v
 +-------------------------------------------+ +-------------------------------------+
 |         CONSUMER A: STATUS TRACKER        | |         CONSUMER B: ANALYTICS       |
 |  - In-memory dict: active_orders          | |  - Aggregates orders per restaurant |
 |  - Evicts orders upon DELIVERED           | |  - Counts frequency per status      |
 |  - Thread-safe state_lock                 | |  - Prints snapshots every 15s       |
 |  - Atomic persistence: state.json (10s)   | |  - Completely independent           |
 |  - REST API & Web Server (Port 5000)      | +-------------------------------------+
 +---------------------+---------------------+
                       |
                       | Serves GET /state & UI
                       v
 +-----------------------------------------------------------------------------------+
 |                           LIVE STATUS BOARD (FRONTEND)                            |
 |  - Polls GET /state every 2 seconds                                               |
 |  - Visualizes active orders, 5-stage progress bars, and kitchen metrics           |
 +-----------------------------------------------------------------------------------+
```

---

## Key Features

- **Decoupled Event-Driven Microservices**: Producers and consumers run independently, communicating strictly via Kafka topics.
- **Strict Partition-Level Ordering**: Uses `order_id` as the Kafka message key to guarantee that all status transitions for an order land on the same partition and are processed in FIFO order.
- **Independent Consumer Groups**:
  - `status-tracker`: Maintains active order state, serves a REST API, and atomically persists state to disk.
  - `analytics`: Computes real-time business metrics without affecting Consumer A's processing speed or state.
- **Thread-Safe State & Atomic Disk Persistence**: State transitions and serialization use `threading.Lock()` to prevent race conditions; state dumps use temp file creation and `os.replace` for atomic file replacement.
- **Modern Responsive Dashboard**: Glassmorphic dark UI with live polling, stage counters, 5-step animated progress indicators, and graceful order dismissal.
- **Centralized Environment Configuration**: Fully configurable via environment variables (`KAFKA_BROKER`, `PORT`, `PERSIST_INTERVAL`, etc.).
- **Python 3.12 & Windows Native Compatibility**: Uses `kafka-python-ng` to eliminate legacy socket/SSL selector incompatibilities.
- **100% Passing Automated Unit Test Suite**: Comprehensive tests using mocked Kafka clients for deterministic, offline verification.

---

## Technology Stack

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Message Broker** | Apache Kafka 3.7 (Bitnami) | Distributed, partitioned event log |
| **Coordination** | Apache ZooKeeper 3.9 (Bitnami) | Broker metadata & cluster coordination |
| **Monitoring UI** | Kafdrop | Web interface for inspecting topics & partitions |
| **Backend & CLI** | Python 3.12 / `kafka-python-ng` | Event producer and consumer services |
| **REST API Server**| Flask / `flask-cors` | Exposes `/state` and `/health` endpoints |
| **Frontend** | HTML5, Modern CSS, Vanilla JS | Real-time polling live status board |
| **Testing** | `pytest` | Unit and integration test suite |
| **Containerization**| Docker Compose | Multi-container infrastructure orchestration |

---

## Project Structure

```
Food-delivery-using-AK/
├── docker-compose.yml          # Zookeeper, Kafka, and Kafdrop containers
├── config.py                   # Centralized configuration via environment variables
├── fixtures.py                 # Realistic simulation datasets (26 restaurants, 36 customers, 53 items)
├── producer.py                 # Order generator with retries, validation, and [SENT] logs
├── consumer_status.py          # Consumer A: state tracking, Flask API, atomic state persistence
├── consumer_analytics.py       # Consumer B: independent metrics counters and reporting
├── init_topic.py               # Topic creation script (3 partitions, 1hr retention)
├── requirements.txt            # Python dependencies
├── .gitignore                  # Git ignore rules for state files, cache, and virtual environments
├── frontend/
│   ├── index.html              # Live dashboard markup
│   ├── style.css               # Glassmorphic dark responsive styles
│   └── app.js                  # Polling engine, DOM reconciler, and progress calculations
├── tests/
│   ├── __init__.py
│   ├── test_fixtures.py        # Fixture threshold validations
│   ├── test_config.py          # Environment override validations
│   ├── test_producer.py        # Schema validation, keying, 5 lifecycle steps, CLI args
│   ├── test_consumer_status.py # In-memory state, lifecycle updates, DELIVERED eviction
│   ├── test_persistence.py     # Atomic write and JSON durability
│   ├── test_api.py             # GET /state and GET /health validation
│   └── test_consumer_analytics.py # Independent counter aggregation
├── README.md                   # System documentation & setup guide
└── LEARNINGS.md                # Offset experiment findings & guided architecture checkpoints
```

---

## Message Key Rationale

### Message Key Rationale

When publishing events to an Apache Kafka topic, selecting the appropriate message key is one of the most critical architectural decisions.

In our system, **`order_id` is used as the Kafka message key for every event published to the `order-events` topic.**

#### Why `order_id` is the Only Correct Choice:

1. **Kafka's Partitioning Guarantee**:
   Kafka topics are divided into multiple partitions (in our setup, 3 partitions). When a producer sends a record with a key, Kafka's default partitioner computes a hash of the key:
   $$\text{Partition} = \text{hash}(\text{key}) \pmod{\text{Total Partitions}}$$
   Because the hash of an immutable string is deterministic, **all records sharing the exact same key are guaranteed to be published to the exact same partition**.

2. **Total Ordering is Partition-Scoped**:
   A fundamental tenet of Kafka is that **message order is strictly guaranteed within a partition, but NOT across different partitions**.
   - If no key were provided (round-robin partitioning), the `PLACED`, `CONFIRMED`, `PREPARING`, `OUT_FOR_DELIVERY`, and `DELIVERED` events for order `ORD-001` would be scattered randomly across Partitions 0, 1, and 2.
   - Because consumer instances or threads consume partitions in parallel at slightly varying speeds, a consumer could read and process the `DELIVERED` event *before* the `PLACED` or `PREPARING` event!
   - This would result in invalid state transitions, race conditions, or orders being resurrected after delivery.

3. **Why Not Use `restaurant` or `customer_name`?**:
   - If `restaurant` were the key, all orders for "Bella Napoli" would land on one partition. While this preserves order per restaurant, it introduces severe **partition hot-spotting** (a popular restaurant could overload a single broker partition while others sit idle).
   - If `customer_name` were the key, multiple distinct orders from the same customer would be serialized onto one partition unnecessarily, while different orders from different customers would have no ordering relation anyway.
   - `order_id` represents the exact boundary of the business entity requiring chronological consistency. It achieves both **guaranteed FIFO order for each order's lifecycle** and **uniform hash distribution across all available partitions**.

---

## Setup & Running Instructions

### 1. Prerequisites
- **Python 3.10+** (Tested on Python 3.12)
- **Docker** and **Docker Compose**

### 2. Environment Installation
Clone the repository and install dependencies in a virtual environment:

```bash
# Clone repository
git clone https://github.com/sucha6174/Food-delivery-using-AK.git
cd Food-delivery-using-AK

# Create and activate virtual environment (optional but recommended)
python -m venv venv
# On Windows:
.\venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### 3. Start Kafka Infrastructure with Docker Compose
Start ZooKeeper, Kafka Broker, and Kafdrop in detached mode:

```bash
docker-compose up -d
```

Verify services are healthy:
- **Kafka Broker**: `localhost:9092`
- **ZooKeeper**: `localhost:2181`
- **Kafdrop UI**: Open [http://localhost:9000](http://localhost:9000) in your web browser.

### 4. Create the Kafka Topic
You can create the `order-events` topic using our automated script or Kafka's native CLI.

#### Option A: Using the provided Python script (Recommended)
```bash
python init_topic.py
```

#### Option B: Using Kafka CLI inside the Docker container
```bash
docker-compose exec kafka kafka-topics.sh --create \
  --topic order-events \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092 \
  --config retention.ms=3600000
```

Verify in Kafdrop at [http://localhost:9000](http://localhost:9000) that `order-events` shows:
- **3 Partitions** (0, 1, 2)
- **Config**: `retention.ms = 3600000` (1 hour)

---

### 5. Running the Components

Run each component in a separate terminal window:

#### Terminal 1: Consumer A (Status Tracker & Dashboard Server)
```bash
python consumer_status.py
```
*Consumer A will start listening to Kafka, launch the background persistence thread, and host the REST API / Dashboard on `http://localhost:5000`.*

#### Terminal 2: Consumer B (Real-Time Analytics Engine)
```bash
python consumer_analytics.py
```
*Consumer B will consume `order-events` under the `analytics` group and print aggregate metrics to the terminal every 15 seconds.*

#### Terminal 3: Producer (Order Simulator)
```bash
# Simulate 10 orders (default)
python producer.py

# Or simulate a custom number of orders with customized delays:
python producer.py --orders 5 --delay-min 1.0 --delay-max 3.0
```
*The producer will publish events, outputting `[SENT] ORD-001 -> PLACED` up to `DELIVERED`.*

#### Terminal 4: View the Live Status Board
Open your browser at:
```
http://localhost:5000
```
Watch the live cards update and animate in real-time as events stream through Kafka!

---

## REST API Reference

Consumer A exposes the following HTTP endpoints:

### 1. `GET /state`
Returns the current in-memory dictionary of all active (non-delivered) orders.

- **URL**: `http://localhost:5000/state`
- **Method**: `GET`
- **Response Format**: `application/json`
- **Response Example**:
  ```json
  {
    "ORD-001": {
      "order_id": "ORD-001",
      "customer_name": "Maya Lin",
      "restaurant": "Tokyo Ramen & Izakaya",
      "items": [
        "Tonkotsu Pork Ramen",
        "Chicken Karaage Bites"
      ],
      "status": "PREPARING",
      "timestamp": "2026-09-22T15:10:45.120Z",
      "estimated_delivery_minutes": 25
    }
  }
  ```

### 2. `GET /health`
Service health probe reporting uptime and count of active orders.

- **URL**: `http://localhost:5000/health`
- **Method**: `GET`
- **Response Example**:
  ```json
  {
    "status": "healthy",
    "service": "status-tracker",
    "active_orders_count": 4,
    "timestamp": 1790098400.12
  }
  ```

---

## State Persistence Mechanism

Consumer A persists its in-memory state every 10 seconds to `state.json`.

### Atomic File Write Pattern
To prevent data corruption if the process is terminated during a write, Consumer A follows an atomic write pattern:
1. Acquires `state_lock` and creates a deep snapshot of `active_orders`.
2. Serializes the snapshot to a temporary file: `state.json.tmp`.
3. Flushes and forces write to disk via `os.fsync()`.
4. Executes `os.replace("state.json.tmp", "state.json")`.

On POSIX and modern Windows filesystems, `os.replace` is an atomic filesystem operation. If the operating system crashes or disk fills during step 2, the existing `state.json` remains intact and uncorrupted.

---

## Running the Automated Test Suite

The test suite validates all requirements using mocked Kafka clients. You can run the entire suite locally without needing Docker:

```bash
python -m pytest tests/ -v
```

### Test Coverage Breakdown:
- `test_fixtures.py`: Asserts fixture counts meet `restaurants >= 20`, `customers >= 30`, `menu_items >= 40`.
- `test_config.py`: Asserts environment variables and default values.
- `test_producer.py`: Validates JSON schema, CLI `--orders` handling, message key assignment (`order_id`), and sequential 5-stage publishing.
- `test_consumer_status.py`: Validates thread-safe state dictionary transitions, eviction on `DELIVERED`, and malformed message resilience.
- `test_persistence.py`: Validates atomic file replacement and JSON schema integrity.
- `test_api.py`: Validates Flask endpoints `GET /state`, `GET /health`, and static UI delivery.
- `test_consumer_analytics.py`: Validates restaurant and status counter accuracy and independence.

---

## Troubleshooting Guide

| Problem | Cause | Solution |
| :--- | :--- | :--- |
| `Cannot connect to Kafka at localhost:9092` | Docker container not started or port in use | Run `docker-compose ps` to ensure Kafka is running and port `9092` is accessible. |
| `AttributeError: module 'ssl' has no attribute 'wrap_socket'` | Legacy `kafka-python` package on Python 3.12 | Use `kafka-python-ng` (already configured in `requirements.txt`). |
| Consumer receives no messages | Topic name mismatch or incorrect consumer group | Verify topic `order-events` exists in Kafdrop at `http://localhost:9000`. |
| Port 5000 already in use | AirPlay receiver on macOS or another local service | Run with an environment variable override: `PORT=5050 python consumer_status.py`. |
