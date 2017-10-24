#!/bin/bash --
set -e -u -o pipefail

source ../../../code.justin.tv/stats/scieng-deploy/conf/integration/blueprint.sh
JSON_CONFIG=../../../code.justin.tv/stats/scieng-deploy/conf/integration/blueprint.json
python3 build/scripts/generate_angular_config.py $JSON_CONFIG static/app/environment.js

# copy pasted from the exec statement in build/scripts/run_blueprint.sh,
# switched out to go run main.go, -config pointed to $JSON_CONFIG, -staticFiles
# pointed to static/, -rollbarEnvironment to local-${TF_VAR_namespace}
exec ./blueprint "$@"                                        \
  -enableAuth=${ENABLE_AUTH}                                 \
  -bpdbConnection="${BLUEPRINT_DB_URL}"                      \
  -cookieSecret=${COOKIE_SECRET}                             \
  -clientID=${CLIENT_ID}                                     \
  -clientSecret=${CLIENT_SECRET}                             \
  -githubServer=${GITHUB_SERVER}                             \
  -requiredOrg=${REQUIRED_ORG}                               \
  -adminTeam=${ADMIN_TEAM}                                   \
  -staticfiles="static/"                                     \
  -ingesterURL="${INGESTER_URL}"                             \
  -rollbarToken="${ROLLBAR_TOKEN}"                           \
  -rollbarEnvironment="local-$TF_VAR_namespace"              \
  -slackbotURL="${SLACKBOT_URL}"                             \
  -config="${JSON_CONFIG}"
