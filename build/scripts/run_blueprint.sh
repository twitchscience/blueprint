#!/bin/bash --
set -e -u -o pipefail

SCIENCE_DIR="/opt/science"
CONFIG_DIR="${SCIENCE_DIR}/blueprint/config"

cd -- "$(dirname -- "$0")"

eval "$(curl 169.254.169.254/latest/user-data/)"

exec ./blueprint \
  -scoopURL="${SCOOP_URL}" \
  -transformConfig="${CONFIG_DIR}/transforms_available.json" \
  -staticfiles="${SCIENCE_DIR}/nginx/html"
