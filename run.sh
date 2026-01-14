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
  --name onlog-msk-producer \
  --restart unless-stopped \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  -e KAFKA_BOOTSTRAP_SERVERS \
  -e DB_BASE_PATH \
  -e TOPIC_PREFIX \
  -v "$DB_BASE_PATH:$DB_BASE_PATH:ro" \
  onlog/msk-producer:latest

echo "[4/4] Done"
docker ps | grep $CONTAINER
