#!/usr/bin/env bash

set -euo pipefail

CI_FILE=$1
VARS_WHITELIST_FILE=$(realpath "${BASH_SOURCE%/*}/../config/trino-ci-vars-whitelist.txt")

{
    grep --only-matching 'vars\.[[:alnum:]][[:alnum:]_]*' "$CI_FILE" || true
} |
    sed -e 's/vars\.//' |
    sort --unique |
    { grep -v -x -f "${VARS_WHITELIST_FILE}" || [ $? -eq 1 ] ; } >ci_vars.txt
if grep -q '[^[:space:]]' ci_vars.txt; then
    echo "Following unexpected variables found in $CI_FILE:"
    cat ci_vars.txt
    echo
    echo "This probably means we need to adapt workflow to use the starburst infrastructure."
    exit 1
fi
