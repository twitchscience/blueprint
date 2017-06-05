#!/bin/bash --
set -euo pipefail

test_path=test_results
coverage_path=${test_path}/coverage
results_path=${test_path}/results

mkdir -p ${coverage_path}
mkdir -p ${results_path}

SRCDIRS=$(go list ./... | grep -v /vendor/)

for pkg in $SRCDIRS; do
  echo "Testing and generating coverage for: ${pkg}"
  pkg_name=${pkg//\//_}
  coverage_filename=${coverage_path}/${pkg_name}.out
  report_filename=${results_path}/${pkg_name}.xml
  go test -race -coverprofile=${coverage_filename} ${pkg} -v | go-junit-report > ${report_filename}
  if [[ -f ${coverage_filename} ]]; then
    go tool cover -func=${coverage_filename}
  fi
done
