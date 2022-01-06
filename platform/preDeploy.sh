#!/usr/bin/env bash
set -euo pipefail

# download a terraform binary
TERRAFORM_VERSION=0.13.7
TERRAFORM_DOWNLOAD_URL="https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip"
curl -L "${TERRAFORM_DOWNLOAD_URL}" > /tmp/terraform.zip
TMPDIR=`mktemp -d`
unzip /tmp/terraform.zip -d "${TMPDIR}"

TERRAFORM="${TMPDIR}/terraform"

# put the required terraform files here
cd /terraform/kafka

CLIENT_CERT_PATH=`mktemp`
PRIVATE_KEY_PATH=`mktemp`

echo "${KAFKA_CLIENT_CERT}" > "${CLIENT_CERT_PATH}"
echo "${KAFKA_PRIVATE_KEY}" > "${PRIVATE_KEY_PATH}"
export TF_VAR_bootstrap_servers=$(echo $KAFKA_BOOTSTRAP_SERVERS | sed -e 's/^/["/' -e 's/,/","/g' -e 's/$/"]/')
export TF_VAR_trust_store_path="${TRUSTSTORE_LOCATION}"
export TF_VAR_client_cert_path="${CLIENT_CERT_PATH}"
export TF_VAR_client_key_path="${PRIVATE_KEY_PATH}"
export TF_WORKSPACE="${AWS_ENVIRONMENT}"
export TF_IN_AUTOMATION=1

# initialize backend
"${TERRAFORM}" init -input=false

# tf apply no questions asked
"${TERRAFORM}" apply -auto-approve -input=false
