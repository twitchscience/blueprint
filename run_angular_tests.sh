#!/bin/bash
set -e -u -o pipefail

cd -- "$(dirname -- "$0")/static"
npm install
npm run test-single-run
