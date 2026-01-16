#!/bin/bash
set -euo pipefail

######################################
# 0. MODE 결정 (arg > env > default)
######################################
MODE="${1:-}"

######################################
# 1. .env 생성
######################################
if [ ! -f .env ]; then
  echo "[INFO] .env not found. Creating from env.example"
  cp env.example .env
fi

######################################
# 2. env 로드
######################################
set -a
source .env
set +a

######################################
# 3. PRODUCER_MODE 결정
######################################
if [ -n "$MODE" ]; then
  PRODUCER_MODE="$MODE"
else
  PRODUCER_MODE="${PRODUCER_MODE:-realtime}"
fi

######################################
# 4. 필수 변수 검증
######################################
: "${KAFKA_BOOTSTRAP_SERVERS:?}"
: "${DB_BASE_PATH:?}"
: "${PRODUCER_MODE:?}"

if [[ "$PRODUCER_MODE" != "realtime" && "$PRODUCER_MODE" != "backfill" ]]; then
  echo "[ERROR] PRODUCER_MODE must be 'realtime' or 'backfill'"
  exit 1
fi

######################################
# 5. restart policy 결정
######################################
RESTART_POLICY="unless-stopped"
if [ "$PRODUCER_MODE" = "backfill" ]; then
  RESTART_POLICY="no"
fi

######################################
# 6. Docker 설정
######################################
IMAGE=onlog/msk-producer:latest
CONTAINER=onlog-msk-producer-${PRODUCER_MODE}

echo "[MODE] $PRODUCER_MODE"
echo "[DB ] $DB_BASE_PATH"
echo "[RST ] $RESTART_POLICY"

docker build -t $IMAGE .

docker rm -f $CONTAINER 2>/dev/null || true

docker run -d \
  --name $CONTAINER \
  --restart $RESTART_POLICY \
  -e AWS_REGION=ap-northeast-2 \
  -e KAFKA_BOOTSTRAP_SERVERS \
  -e DB_BASE_PATH \
  -e PRODUCER_MODE \
  -e BACKFILL_CUTOFF \
  -v /home/ubuntu/.aws:/root/.aws:ro \
  -v "$DB_BASE_PATH:$DB_BASE_PATH" \
  $IMAGE

docker ps | grep $CONTAINER || true
