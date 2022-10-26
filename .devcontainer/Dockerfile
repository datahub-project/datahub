FROM uchimera.azurecr.io/cccs/dev/datahub-build:feature_CLDN-1684

# Initialize custom CA bundle for Python and Node
ENV CURL_CA_BUNDLE="/usr/local/share/python-ca-bundle/cabundle.crt" \
    REQUESTS_CA_BUNDLE="/usr/local/share/python-ca-bundle/cabundle.crt" \
    NODE_EXTRA_CA_CERTS="/usr/local/share/python-ca-bundle/cabundle.crt"

# Add custom CA certs
ARG extraCaCertsDir=".work"
ADD ${extraCaCertsDir} /usr/local/share/ca-certificates/

# Add custom CA certs to the system bundle, Java, Python, Node, etc.
RUN update-ca-certificates \
    && mkdir $(dirname "${CURL_CA_BUNDLE}") \
    && cp "$(python3 -m certifi)" "${CURL_CA_BUNDLE}" \
    # For some reason at this point, JAVA_HOME has been messed up by sdkman, so figure out where it's installed
    && JAVA_DIR=$(readlink -f "$(dirname $(which java))/../") \
    && for cert in /usr/local/share/ca-certificates/*; do \
        openssl x509 -outform der -in "$cert" -out /tmp/certificate.der; \
        ${JAVA_DIR}/bin/keytool -import -alias "$cert" -keystore ${JAVA_DIR}/lib/security/cacerts -file /tmp/certificate.der -deststorepass changeit -noprompt; \
        cat "${cert}" >>"${CURL_CA_BUNDLE}"; \
    done \
    && python3 -m pip config --global set global.cert "${CURL_CA_BUNDLE}" \
    && npm config --global set cafile "${CURL_CA_BUNDLE}" \
    && rm /tmp/certificate.der
