#!/bin/bash

# This script builds and pushes the Docker images for the AutoTrader system to Docker Hub.
#
# Prerequisites:
# - Docker is installed and running.
# - You are logged into Docker Hub using `docker login`.
#
# Usage:
# ./build_and_push_images.sh <dockerhub_username> [tag]
#   - <dockerhub_username>: Your Docker Hub username.
#   - [tag]: (Optional) The tag for the images (e.g., "latest", "v1.0"). Defaults to "latest".

set -e

DOCKERHUB_USERNAME=$1
TAG=${2:-latest}

if [ -z "$DOCKERHUB_USERNAME" ]; then
  echo "Error: Docker Hub username is required."
  echo "Usage: ./build_and_push_images.sh <dockerhub_username> [tag]"
  exit 1
fi

echo "Building and pushing images for user: $DOCKERHUB_USERNAME with tag: $TAG"

# Build and push Master image
echo "Building master image..."
docker buildx build --platform linux/amd64 -t ${DOCKERHUB_USERNAME}/autotrader-master:${TAG} -f MasterVM/Dockerfile . --load
docker push ${DOCKERHUB_USERNAME}/autotrader-master:${TAG}

# Build and push Data Fetcher image
echo "Building data-fetcher image..."
docker buildx build --platform linux/amd64 -t ${DOCKERHUB_USERNAME}/autotrader-data-fetcher:${TAG} -f SlaveVM/data_fetcher/Dockerfile . --load
docker push ${DOCKERHUB_USERNAME}/autotrader-data-fetcher:${TAG}

# Build and push Kline Websocket image
echo "Building kline-websocket image..."
docker buildx build --platform linux/amd64 -t ${DOCKERHUB_USERNAME}/autotrader-kline-websocket:${TAG} -f SlaveVM/websockets/Dockerfile.kline . --load
docker push ${DOCKERHUB_USERNAME}/autotrader-kline-websocket:${TAG}

# Build and push Liquidation Websocket image
echo "Building liquidation-websocket image..."
docker buildx build --platform linux/amd64 -t ${DOCKERHUB_USERNAME}/autotrader-liquidation-websocket:${TAG} -f SlaveVM/websockets/Dockerfile.liquidation . --load
docker push ${DOCKERHUB_USERNAME}/autotrader-liquidation-websocket:${TAG}

# Build and push Health Checker image
echo "Building health-checker image..."
docker buildx build --platform linux/amd64 -t ${DOCKERHUB_USERNAME}/autotrader-health-checker:${TAG} -f SlaveVM/health_checker/Dockerfile . --load
docker push ${DOCKERHUB_USERNAME}/autotrader-health-checker:${TAG}

# Build and push Config Updater image
echo "Building config-updater image..."
docker buildx build --platform linux/amd64 -t ${DOCKERHUB_USERNAME}/autotrader-config-updater:${TAG} -f Scripts/deployment/Dockerfile.config-updater . --load
docker push ${DOCKERHUB_USERNAME}/autotrader-config-updater:${TAG}
echo "Pushing config-updater image..."
docker push ${DOCKERHUB_USERNAME}/autotrader-config-updater:${TAG}

echo "All images have been successfully built and pushed to Docker Hub."
