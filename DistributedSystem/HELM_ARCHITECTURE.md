# Helm Chart Architecture for the Distributed Trader System

This document provides an overview of the Kubernetes-native architecture for the Distributed Trader system, managed by a Helm chart. This setup is designed for scalability, resilience, and automated management.

## Core Concepts

The system is deployed to a Kubernetes cluster (such as k3s) and is composed of the following core components, all managed by the `autotrader` Helm chart:

- **Master Node:** A Kubernetes `Deployment` that runs a single instance of the master coordinator. It is responsible for monitoring the health of the slave pods.
- **Slave Nodes:** A Kubernetes `StatefulSet` that manages the slave pods. Each pod contains the four required containers (`data-fetcher`, `kline-websocket`, `liquidation-websocket`, `health-checker`). The number of slave pods can be easily scaled.
- **MongoDB:** The database is treated as an external dependency. The Helm chart is configured to connect to a MongoDB instance running outside the Kubernetes cluster.
- **Configuration:** All configuration is managed through Kubernetes native resources:
    - `ConfigMaps` are used for non-sensitive data, such as environment variables and the dynamically updated symbol distribution lists.
    - `Secrets` are used for sensitive data, specifically the MongoDB connection credentials.
- **Automated Symbol Distribution:** A Kubernetes `CronJob` runs on a configurable schedule to automatically keep the trading symbol lists up-to-date.

## Symbol Distribution: A Fully Automated, Cloud-Native Approach

The symbol distribution process is now fully automated by a Kubernetes `CronJob`, ensuring the system always has the latest trading symbols from Binance without any manual intervention.

Here's how it works:

1.  **Scheduled Execution:** The `CronJob`, named `config-updater`, runs on a schedule defined in `values.yaml` (e.g., daily at midnight).
2.  **Dedicated Service Account:** The job runs with a dedicated `ServiceAccount` that has specific RBAC permissions to manage `ConfigMap`s and `StatefulSet`s, following the principle of least privilege.
3.  **Dynamic Symbol Fetching:** The `config-updater` pod runs a Python script that:
    a.  Connects to the Kubernetes API from within the cluster.
    b.  Determines the current number of slave replicas from the slave `StatefulSet`.
    c.  Fetches the latest list of active USDT futures symbols from the Binance API.
    d.  Distributes these symbols evenly among the number of running slaves.
4.  **Live ConfigMap Updates:** The script directly creates or updates the required `ConfigMap` for each slave pod via the Kubernetes API.
5.  **Automatic Pod Restart:** After the `ConfigMap`s are updated, the script triggers a rolling restart of the slave `StatefulSet`. This is a crucial step that forces the slave pods to terminate and restart, ensuring they mount the updated `ConfigMap`s and start collecting data for the new symbol lists.

This automated approach provides significant advantages:
- **Zero-Touch Operation:** No manual scripts to run or configurations to apply. The system is self-maintaining.
- **Resilience:** The system automatically adapts to new symbols being listed or delisted by Binance.
- **Scalability:** The process automatically adapts to changes in the number of slave replicas.

## Deployment Workflow

Here is the simplified, end-to-end workflow for deploying the Distributed Trader system to Kubernetes:

**Step 1: Build and Push Docker Images**

Before deploying, you need to build the Docker images for each component (including the new `config-updater`) and push them to a container registry.

1.  **Log in to Docker Hub:**
    ```bash
    docker login
    ```

2.  **Run the build and push script:**
    ```bash
    chmod +x build_and_push_images.sh
    ./build_and_push_images.sh <your-dockerhub-username> [tag]
    ```
    For example:
    ```bash
    ./build_and_push_images.sh w24351789 latest
    ```

**Step 2: Configure the Helm Chart**

Edit the `helm/autotrader/values.yaml` file to match your environment. The most important values to change are:
- `slave.replicaCount`: The number of slave pods you want to run.
- `mongodb.uri`: The connection string for your external MongoDB database.
- `mongodb.root.user` and `mongodb.root.password`: The credentials for your MongoDB database.
- `cronjob.schedule`: The schedule for the automatic symbol updates (e.g., `"0 0 * * *"` for daily at midnight).

**Step 3: Deploy the Helm Chart**

1.  **Navigate to the Helm chart directory:**
    ```bash
    cd helm/autotrader
    ```

2.  **Perform a dry run to validate the templates (optional but recommended):**
    ```bash
    helm install autotrader . --dry-run --debug
    ```

3.  **Install the Helm chart:**
    ```bash
    helm install autotrader .
    ```
The Helm chart will create all the necessary resources, including the `CronJob`. The `CronJob` will run for the first time according to its schedule and will automatically create the initial `ConfigMap`s for the symbols.

**Step 4: Accessing the Application**

After the deployment is complete, you can access the master dashboard using `kubectl port-forward`, as described in the `NOTES.txt` file that is displayed after a successful installation.

## Managing the Deployment

- **Updating the application:** To update the application with new code, rebuild and push your Docker images with a new tag, update the image tag(s) in your `values.yaml`, and then run `helm upgrade autotrader .`.
- **Scaling the slaves:** To scale the number of slaves, simply update the `slave.replicaCount` in `values.yaml` and run `helm upgrade autotrader .`. The `CronJob` will automatically detect the new replica count on its next run and adjust the symbol distribution accordingly.
- **Uninstalling the application:**
    ```bash
    helm uninstall autotrader
    ```
    This will also remove the `CronJob` and the `ConfigMap`s it created.
