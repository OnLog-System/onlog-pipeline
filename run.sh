#!/bin/bash
set -e
set -a
source .env
set +a

IMAGE=onlog/msk-producer:latest
CONTAINER=onlog-msk-producer

echo "[1/4] Build image"
docker build -t $IMAGE .

echo "[2/4] Stop existing container (if any)"
docker rm -f $CONTAINER 2>/dev/null || true

echo "[3/4] Run container"
docker run -d \
  --name $CONTAINER \
  --restart unless-stopped \
  -e KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e DB_BASE_PATH="$DB_BASE_PATH" \
  -e TOPIC_PREFIX="$TOPIC_PREFIX" \
  -v "$DB_BASE_PATH:$DB_BASE_PATH:ro" \
  $IMAGE

echo "[4/4] Done"
docker ps | grep $CONTAINER
