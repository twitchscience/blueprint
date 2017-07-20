#!/bin/bash
set -e -u -o pipefail
cd static
npm install
npm run test-single-run
