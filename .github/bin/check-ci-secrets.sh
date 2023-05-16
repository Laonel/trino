#!/usr/bin/env bash

set -euo pipefail

CI_FILE=$1

cat > known_secrets.txt <<EOF
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
CICD_AWS_ACCESS_KEY_ID
CICD_AWS_SECRET_ACCESS_KEY
ENGINEERING_AWS_ACCESS_KEY_ID
ENGINEERING_AWS_SECRET_ACCESS_KEY
ENGINEERING_TESTS_AWS_ACCESS_KEY
ENGINEERING_TESTS_AWS_SECRET_KEY
ENG_AWS_ACCESS_KEY_ID
ENG_AWS_SECRET_ACCESS_KEY
GITHUB_TOKEN
S3_TESTS_AWS_ACCESS_KEY
S3_TESTS_AWS_SECRET_KEY
SECRETS_PRESENT
EOF

grep --only-matching 'secrets\.[[:alnum:]][[:alnum:]_]*' "$CI_FILE" \
    | sed -e 's/secrets\.//' \
    | sort --unique > ci_secrets.txt
comm -23 ci_secrets.txt known_secrets.txt > extra_secrets.txt
if grep -q '[^[:space:]]' extra_secrets.txt; then
    echo "Following unexpected secrets references found in $CI_FILE:"
    cat extra_secrets.txt
    echo
    echo "This probably means we need to adapt workflow to use the corresponding starburst secret."
    exit 1
fi
