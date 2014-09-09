#!/bin/bash --
set -e -u -o pipefail

SCIENCE_DIR="/opt/science"
CONFIG_DIR="${SCIENCE_DIR}/blueprint/config"

cd -- "$(dirname -- "$0")"

eval "$(curl 169.254.169.254/latest/user-data/)"

exec ./blueprint \
  -hostname=${SCOOP_HOSTNAME} \
  -proto=${SCOOP_PROTO} \
  -port=${SCOOP_PORT} \
  -transformConfig="${CONFIG_DIR}/transforms_available.json" \
  -staticfiles="${SCIENCE_DIR}/nginx/html"

