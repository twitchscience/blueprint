#!/bin/bash --
set -e -u -o pipefail

CONFIG_DIR="/opt/science/blueprint/config"
SCIENCE_DIR="/opt/science"

cd -- "$(dirname -- "$0")"

eval "$(curl 169.254.169.254/latest/user-data/)"

exec ./schema_suggestor \
  -hostname=${SCOOP_HOSTNAME} \
  -proto=${SCOOP_PROTO} \
  -port=${SCOOP_PORT} \
  -transformConfig="${CONFIG_DIR}/transforms_available.json" \
  -staticfiles="${SCIENCE_DIR}/nginx/html/events"
