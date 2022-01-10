#!/usr/bin/env bash

set -euo pipefail

AWS_ENVIRONMENT=${AWS_ENVIRONMENT:?make sure to aws environment}

BUILDDIR=`mktemp -d`
pushd $BUILDDIR

# clone and build
echo "cloning and building amazon's tool"
git clone https://github.com/aws-samples/amazon-msk-client-authentication
cd amazon-msk-client-authentication
# TODO: work on osx too
mvn -Djavax.net.ssl.trustStore=KeychainStore clean package -f pom.xml

KSA_ALIAS=event-journal-server`date +%Y-%m-%d`
DISTINGUISHED_NAME=event-journal-server
VAULT_PATH=secret/dataportal
set +eo pipefail
KEYSTORE_PASSWORD=`cat /dev/urandom | base64 | head -c24`
set -eo pipefail
KEYSTORE_PATH=/tmp/kafka.client.keystore.jks
PRIVATE_KEY_PATH=/tmp/kafka-private-key.pem
CERT_PATH=/tmp/signed-certificate-from-acm

# as of now we have one CA, can just grab first element
echo "getting certificate authority arn from aws"
CERTIFICATE_AUTHORITY_ARN=$(aws acm-pca list-certificate-authorities | jq -r '.CertificateAuthorities[0].Arn')

echo "using amazon's tool to generate the certs"
java -jar target/AuthMSK-1.0-SNAPSHOT.jar \
-caa $CERTIFICATE_AUTHORITY_ARN \
-ksl $KEYSTORE_PATH \
-ksp $KEYSTORE_PASSWORD \
-ksa $KSA_ALIAS \
-dgn $DISTINGUISHED_NAME \
-pem \
-pkf $PRIVATE_KEY_PATH \
-ccf $CERT_PATH
KEYSTORE_PATH_B64="${KEYSTORE_PATH}.b64"
cat $KEYSTORE_PATH | base64 | tr -d '\n' > $KEYSTORE_PATH_B64

cleanup() {
    echo "cleaning up!"
    rm $KEYSTORE_PATH $KEYSTORE_PATH_B64 $PRIVATE_KEY_PATH $CERT_PATH
    rm -rf $BUILDDIR
}
trap cleanup EXIT

vault-token ()
{
    export VAULT_TOKEN="$(get-vault-token)";
    if [[ ! -n "$VAULT_TOKEN" ]]; then
        return 1;
    fi;
}
vault-token

# store in vault, make sure to include any other secrets needed under the path
# SPRING_* is used for setting up dataportal-gme
# KAFKA_* is used in terraform script
echo "storing in vault"
vault write "$VAULT_PATH" \
KAFKA_KEY_STORE_PASSWORD="$KEYSTORE_PASSWORD" \
KAFKA_BASE64_ENCODED_KEY_STORE=@$KEYSTORE_PATH_B64 \
KAFKA_PRIVATE_KEY=@$PRIVATE_KEY_PATH \
KAFKA_CLIENT_CERT=@$CERT_PATH\
SCHEMA_REGISTRY_TYPE="AWS_GLUE" \
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL="SSL" \
SPRING_KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION="/tmp/mykeystore" \
SPRING_KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD="$KEYSTORE_PASSWORD" \
SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD="changeit"\
SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION="/usr/lib/jvm/java-1.8-openjdk/jre/lib/security/cacerts"\
TRUSTSTORE_LOCATION="/usr/lib/jvm/java-1.8-openjdk/jre/lib/security/cacerts"
