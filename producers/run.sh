#!/bin/bash
set -euo pipefail

######################################
# 0. .env 생성
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
# 2. 필수 변수 검증
######################################
: "${KAFKA_BOOTSTRAP_SERVERS:?}"
: "${DB_BASE_PATH:?}"
: "${PRODUCER_MODE:=realtime}"

######################################
# 3. Docker 설정
######################################
IMAGE=onlog/msk-producer:latest
CONTAINER=onlog-msk-producer-${PRODUCER_MODE}

echo "[MODE] $PRODUCER_MODE"

docker build -t $IMAGE .

docker rm -f $CONTAINER 2>/dev/null || true

# ---- mode별 run 옵션 ----
if [ "$PRODUCER_MODE" = "realtime" ]; then
  RESTART_OPT="--restart unless-stopped"
else
  RESTART_OPT=""
fi

docker run -d \
  --name $CONTAINER \
  $RESTART_OPT \
  -e AWS_REGION=ap-northeast-2 \
  -e KAFKA_BOOTSTRAP_SERVERS \
  -e DB_BASE_PATH \
  -e PRODUCER_MODE \
  -v /home/ubuntu/.aws:/root/.aws:ro \
  -v "$DB_BASE_PATH:$DB_BASE_PATH:ro" \
  $IMAGE

docker ps | grep $CONTAINER || true
