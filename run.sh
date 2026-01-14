#!/bin/bash
set -euo pipefail

######################################
# 0. .env 자동 생성 (최초 1회)
######################################
if [ ! -f .env ]; then
  echo "[INFO] .env not found. Creating from env.example"
  cp env.example .env
fi

######################################
# 1. env 로드
######################################
set -a
source .env
set +a

######################################
# 2. 필수 환경변수 검증
######################################
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
: "${DB_BASE_PATH:?DB_BASE_PATH is required}"
: "${TOPIC_PREFIX:?TOPIC_PREFIX is required}"

######################################
# 3. Docker 설정
######################################
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
  -e AWS_REGION=ap-northeast-2 \
  -e KAFKA_BOOTSTRAP_SERVERS \
  -e DB_BASE_PATH \
  -e TOPIC_PREFIX \
  -v /home/ubuntu/.aws:/root/.aws:ro \
  -v "$DB_BASE_PATH:$DB_BASE_PATH:ro" \
  $IMAGE

echo "[4/4] Done"
docker ps | grep $CONTAINER
