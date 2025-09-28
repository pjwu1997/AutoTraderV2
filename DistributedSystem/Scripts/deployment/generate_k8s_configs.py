#!/usr/bin/env python3
"""
Kubernetes ConfigMap Generator for Symbol Distribution (In-Cluster Version)

This script runs as a Kubernetes CronJob. It fetches all available USDT 
futures symbols from Binance, divides them among the slave pods, and directly
creates or updates the Kubernetes ConfigMap for each slave.

After updating the ConfigMaps, it triggers a rolling restart of the slave
StatefulSet to apply the new configuration.
"""

import os
import sys
import ccxt
import yaml
import logging
import time
from datetime import datetime
from kubernetes import client, config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Environment Variables ---
# These will be set in the CronJob definition in the Helm chart
NAMESPACE = os.getenv("K8S_NAMESPACE", "default")
SLAVE_STATEFULSETS = [
    os.getenv("K8S_SLAVE_DATA_FETCHER_STATEFULSET", "autotrader-slave-data-fetcher"),
    os.getenv("K8S_SLAVE_KLINE_WEBSOCKET_STATEFULSET", "autotrader-slave-kline-websocket"),
    os.getenv("K8S_SLAVE_LIQUIDATION_WEBSOCKET_STATEFULSET", "autotrader-slave-liquidation-websocket"),
]
CONFIGMAP_BASENAME = os.getenv("K8S_CONFIGMAP_BASENAME", "autotrader-slave-symbols")
CHART_NAME = os.getenv("HELM_CHART_NAME", "autotrader")


def load_k8s_config():
    """Loads Kubernetes configuration, either in-cluster or from kubeconfig."""
    try:
        config.load_incluster_config()
        logging.info("Loaded in-cluster Kubernetes config.")
    except config.ConfigException:
        logging.warning("Could not load in-cluster config, falling back to kubeconfig.")
        config.load_kube_config()
        logging.info("Loaded Kubernetes config from kubeconfig.")

def get_slave_replica_count(apps_v1_api) -> int:
    """Gets the number of replicas from the first slave StatefulSet."""
    statefulset_name = SLAVE_STATEFULSETS[0]
    logging.info(f"Fetching replica count from StatefulSet '{statefulset_name}' in namespace '{NAMESPACE}'...")
    try:
        statefulset = apps_v1_api.read_namespaced_stateful_set(name=statefulset_name, namespace=NAMESPACE)
        replicas = statefulset.spec.replicas
        logging.info(f"Found {replicas} replicas in StatefulSet.")
        return replicas
    except client.ApiException as e:
        logging.error(f"Failed to read StatefulSet '{statefulset_name}': {e}")
        sys.exit(1)

def fetch_binance_usdt_futures_symbols():
    """Fetches all USDT-margined futures symbols from Binance."""
    logging.info("Fetching USDT futures symbols from Binance...")
    try:
        exchange = ccxt.binance({'options': {'defaultType': 'future'}})
        markets = exchange.load_markets()
        symbols = [
            market['symbol']
            for market in markets.values()
            if market['quote'] == 'USDT' and market.get('active', True)
        ]
        logging.info(f"Successfully fetched {len(symbols)} active USDT futures symbols.")
        return sorted(symbols)
    except Exception as e:
        logging.error(f"Failed to fetch symbols from Binance: {e}")
        sys.exit(1)

def distribute_symbols(symbols, num_slaves):
    """Distributes a list of symbols evenly among a number of slaves."""
    if num_slaves <= 0:
        logging.error("Number of slaves must be a positive integer.")
        sys.exit(1)
    
    logging.info(f"Distributing {len(symbols)} symbols among {num_slaves} slaves.")
    distribution = [[] for _ in range(num_slaves)]
    for i, symbol in enumerate(symbols):
        distribution[i % num_slaves].append(symbol)
    
    for i, slave_symbols in enumerate(distribution):
        logging.info(f"Slave {i} assigned {len(slave_symbols)} symbols.")
        
    return distribution

def clean_symbol(symbol):
    """Cleans the symbol by removing ':USDT' suffix and '/' character, then converts to uppercase."""
    cleaned = symbol.split(':')[0]  # Remove :USDT suffix
    cleaned = cleaned.replace('/', '') # Remove / character
    return cleaned.upper() # Convert to uppercase

def create_or_update_configmap(core_v1_api, slave_index, symbols_list):
    """Creates or updates a Kubernetes ConfigMap for a slave."""
    configmap_name = f"{CONFIGMAP_BASENAME}-{slave_index}"
    cleaned_symbols = [clean_symbol(s) for s in symbols_list]
    symbols_str = ",".join(cleaned_symbols)
    
    metadata = {
        "name": configmap_name,
        "labels": {
            "app.kubernetes.io/name": CHART_NAME,
            "app.kubernetes.io/component": "slave-symbols",
            "app.kubernetes.io/instance-part": str(slave_index),
        }
    }
    data = {"symbols.csv": symbols_str}
    body = client.V1ConfigMap(api_version="v1", kind="ConfigMap", metadata=metadata, data=data)

    try:
        # Check if the ConfigMap already exists
        core_v1_api.read_namespaced_config_map(name=configmap_name, namespace=NAMESPACE)
        # If it exists, replace it
        logging.info(f"ConfigMap '{configmap_name}' already exists. Updating...")
        core_v1_api.replace_namespaced_config_map(name=configmap_name, namespace=NAMESPACE, body=body)
        logging.info(f"Successfully updated ConfigMap '{configmap_name}'.")
    except client.ApiException as e:
        if e.status == 404:
            # If it doesn't exist, create it
            logging.info(f"ConfigMap '{configmap_name}' not found. Creating...")
            core_v1_api.create_namespaced_config_map(namespace=NAMESPACE, body=body)
            logging.info(f"Successfully created ConfigMap '{configmap_name}'.")
        else:
            # Handle other API errors
            logging.error(f"Failed to create or update ConfigMap '{configmap_name}': {e}")
            sys.exit(1)

def trigger_statefulset_rollout(apps_v1_api):
    """Triggers a rolling restart of the slave StatefulSets."""
    for statefulset_name in SLAVE_STATEFULSETS:
        logging.info(f"Triggering rolling restart for StatefulSet '{statefulset_name}'...")
        patch_body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": datetime.utcnow().isoformat() + "Z"
                        }
                    }
                }
            }
        }
        try:
            apps_v1_api.patch_namespaced_stateful_set(
                name=statefulset_name,
                namespace=NAMESPACE,
                body=patch_body
            )
            logging.info(f"Successfully triggered rolling restart for StatefulSet '{statefulset_name}'.")
        except client.ApiException as e:
            logging.error(f"Failed to trigger rolling restart for StatefulSet '{statefulset_name}': {e}")
            # We don't exit here, so we can try to restart the other statefulsets
    
def main():
    """Main execution function."""
    load_k8s_config()
    
    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    
    # Get the number of slaves
    num_slaves = get_slave_replica_count(apps_v1)
    
    # Fetch and distribute symbols
    all_symbols = fetch_binance_usdt_futures_symbols()
    symbol_distribution = distribute_symbols(all_symbols, num_slaves)
    
    # Create or update ConfigMap for each slave
    for i, slave_symbols in enumerate(symbol_distribution):
        create_or_update_configmap(core_v1, i, slave_symbols)
        
    # Trigger a rollout of the slave StatefulSet to pick up the new ConfigMaps
    trigger_statefulset_rollout(apps_v1)
    
    logging.info("Symbol distribution and slave rollout completed successfully.")

if __name__ == "__main__":
    main()
