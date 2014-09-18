#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"

# If this doesn't work, install spade/transform_dumper
transformer_dumper -outFile gitignore-transforms_available.json

exec ../blueprint \
  -hostname=localhost \
  -proto=http         \
  -port=8080          \
  -transformConfig="gitignore-transforms_available.json" \
  -staticfiles="static/"

