# GEMINI Analysis of the Distributed Trader System

This document provides a comprehensive overview of the Distributed Trader System, an AI-generated analysis based on the project's source code and documentation.

## Project Overview

This project is a distributed data collection system designed to gather cryptocurrency market data from the Binance exchange in a scalable and resilient manner. It employs a Master/Slave architecture to distribute the data collection workload across multiple Virtual Machines (VMs), each with a unique IP address to circumvent API rate limiting.

The system is built using Python and leverages Docker and Docker Compose for containerization and orchestration. Data is stored in a shared MongoDB database.

### Core Technologies

*   **Programming Language:** Python 3
*   **Key Libraries:**
    *   `aiohttp`: For the asynchronous Master Coordinator web server.
    *   `ccxt`: For interacting with the Binance API.
    *   `pymongo`: For connecting to the MongoDB database.
    *   `websockets`: For real-time data collection.
*   **Database:** MongoDB
*   **Containerization:** Docker, Docker Compose

## System Architecture

The system consists of a central Master VM and multiple Slave VMs.

### Master VM

The Master VM serves as the central coordinator and monitoring hub. Its primary responsibilities include:

*   **Symbol Distribution:** Assigning trading symbols to each Slave VM for data collection.
*   **Health Monitoring:** Tracking the status and health of each Slave VM through a heartbeat mechanism.
*   **Dashboard & API:** Providing a web-based dashboard and a RESTful API for monitoring the overall system status.

The core logic of the Master VM is implemented in `MasterVM/src/master_coordinator.py`.

### Slave VMs

Each Slave VM is responsible for collecting data for a specific set of trading symbols assigned by the Master. To ensure high availability and detailed data collection, each Slave VM runs four distinct Docker containers:

*   **`data-fetcher`:** Collects data via the REST API, focusing on 1-minute interval data such as OHLCV, CVD, funding rates, and long/short ratios.
*   **`kline-websocket`:** Streams real-time K-line (candlestick) data via WebSockets.
*   **`liquidation-websocket`:** Streams real-time liquidation data via WebSockets.
*   **`health-checker`:** Monitors the health of the other containers on the Slave VM and reports back to the Master.

The primary data collection logic for the slaves is in `SlaveVM/data_fetcher/unified_collector.py`.

## Data Schema

The system collects a comprehensive range of market data, which is aggregated into 1-minute intervals and stored in a shared MongoDB database. The schema is designed to provide a holistic view of the market for each trading symbol.

A sample document structure from `SYSTEM_ARCHITECTURE.md` is as follows:

```json
{
  "_id": "BTCUSDT_1694640000",
  "timestamp": "2023-09-13T20:00:00Z",
  "symbol": "BTCUSDT",
  "spot": {
    "open": "26500.00",
    "high": "26525.50",
    "low": "26485.25",
    "close": "26515.75",
    "volume": "856.25",
    "cvd": 45.75
  },
  "futures": {
    "open": "26500.00",
    "high": "26525.50",
    "low": "26485.25",
    "close": "26515.75",
    "volume": "1250.5",
    "cvd": 125.75,
    "funding_rate": 0.0001,
    "next_funding_rate": 0.0002,
    "mark_price": 26515.75,
    "index_price": 26512.50
  },
  "long_short_ratio": {
    "global_long_short_ratio": 1.25,
    "top_trader_long_short_ratio": 1.15,
    "taker_buy_sell_ratio": 1.12,
    "open_interest": 125000000
  },
  "liquidations": {
    "buy_liquidations": {
      "total_quantity": 125.5,
      "total_dollars": 3326375.0,
      "event_count": 23
    },
    "sell_liquidations": {
      "total_quantity": 89.25,
      "total_dollars": 2365781.25,
      "event_count": 18
    }
  }
}
```

The data models are formally defined in `Common/models/data_models.py`.

## Building and Running

### Local Development

The project includes a `docker-compose.local.yml` file for setting up a local development environment. This configuration spins up a MongoDB instance, a Master container, and three Slave containers.

To run the system locally, execute the following command:

```bash
docker-compose -f docker-compose.local.yml up
```

### Production Deployment

The `README.md` file provides detailed instructions for deploying the system to production on a set of VMs. The general steps are as follows:

1.  **Provision VMs:** Set up one Master VM and multiple Slave VMs, each with a public IP address.
2.  **Install Dependencies:** Install Docker and Docker Compose on each VM.
3.  **Deploy Master:** Configure and run the `deploy_master.sh` script on the Master VM.
4.  **Deploy Slaves:** Configure and run the `deploy_slave.sh` script on each Slave VM.

## Monitoring

The system's health and status can be monitored through the following endpoints:

*   **Master Dashboard:** `http://<master-vm-ip>:8080/dashboard.html`
*   **Master API:** `http://<master-vm-ip>:8080/api/status`
*   **Slave Health Check:** `http://<slave-vm-ip>:8081/health`

## Key Files

*   `README.md`: The main entry point for understanding the project, including deployment instructions.
*   `SYSTEM_ARCHITECTURE.md`: Provides a detailed overview of the system architecture.
*   `docker-compose.local.yml`: Defines the local development environment.
*   `MasterVM/src/master_coordinator.py`: The core logic for the Master VM.
*   `SlaveVM/data_fetcher/unified_collector.py`: The primary data collection script for the Slave VMs.
*   `Common/models/data_models.py`: Defines the data models used throughout the system.
*   `deploy_all_applications.sh`: A script for deploying the entire system.
