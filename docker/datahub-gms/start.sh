#!/bin/bash

# Add default URI (http) scheme if needed
if ! echo $NEO4J_HOST | grep -q "://" ; then
    NEO4J_HOST="http://$NEO4J_HOST"
fi

if [[ ! -z $ELASTICSEARCH_USERNAME ]] && [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  AUTH_TOKEN=$(echo -ne "$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD" | base64 --wrap 0)
  ELASTICSEARCH_AUTH_HEADER="Authorization:Basic $AUTH_TOKEN"
fi

# Add default header if needed
if [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  ELASTICSEARCH_AUTH_HEADER="Accept: */*"
fi

if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
  ELASTICSEARCH_PROTOCOL=https
else
  ELASTICSEARCH_PROTOCOL=http
fi

WAIT_FOR_EBEAN=""
if [[ $SKIP_EBEAN_CHECK != true ]]; then
  if [[ $ENTITY_SERVICE_IMPL == ebean ]] || [[ -z $ENTITY_SERVICE_IMPL ]]; then
    WAIT_FOR_EBEAN=" -wait tcp://$EBEAN_DATASOURCE_HOST "
  fi
fi

WAIT_FOR_CASSANDRA=""
if [[ $ENTITY_SERVICE_IMPL == cassandra ]] && [[ $SKIP_CASSANDRA_CHECK != true ]]; then
  WAIT_FOR_CASSANDRA=" -wait tcp://$CASSANDRA_DATASOURCE_HOST "
fi

WAIT_FOR_KAFKA=""
if [[ $SKIP_KAFKA_CHECK != true ]]; then
  WAIT_FOR_KAFKA=" -wait tcp://$(echo $KAFKA_BOOTSTRAP_SERVER | sed 's/,/ -wait tcp:\/\//g') "
fi

WAIT_FOR_NEO4J=""
if [[ $GRAPH_SERVICE_IMPL != elasticsearch ]] && [[ $SKIP_NEO4J_CHECK != true ]]; then
  WAIT_FOR_NEO4J=" -wait $NEO4J_HOST "
fi

OTEL_AGENT=""
if [[ $ENABLE_OTEL == true ]]; then
  OTEL_AGENT="-javaagent:opentelemetry-javaagent.jar "
fi

PROMETHEUS_AGENT=""
if [[ $ENABLE_PROMETHEUS == true ]]; then
  PROMETHEUS_AGENT="-javaagent:jmx_prometheus_javaagent.jar=4318:/datahub/datahub-gms/scripts/prometheus-config.yaml "
fi

# JAR extraction optimization - extract to tmpfs for faster class loading
JAR_EXTRACTION_OPTS=""
if [[ $EXTRACT_JAR_ENABLED == true ]]; then
  WORK_DIR="/tmp/gms/extraction"
  JAR_PATH="/datahub/datahub-gms/bin/war.war"

  # Log WAR size and available resources
  WAR_SIZE_MB=$(du -m "$JAR_PATH" | awk '{print $1}')
  AVAILABLE_RAM=$(free -m | awk '/^Mem:/ {print $7}')
  echo "[STARTUP] JAR extraction enabled. WAR size: ${WAR_SIZE_MB}MB, Available RAM: ${AVAILABLE_RAM}MB"

  if [[ $WAR_SIZE_MB -gt 1000 ]]; then
    echo "[WARN] WAR size (${WAR_SIZE_MB}MB) exceeds tmpfs limit (1Gi). Extraction may fail"
  fi

  if [[ $AVAILABLE_RAM -lt 500 ]]; then
    echo "[WARN] Low available RAM (${AVAILABLE_RAM}MB). Extraction may fail or trigger swap"
  fi

  EXTRACTION_COMPLETE="${WORK_DIR}/.extraction-complete"

  # Idempotent extraction with validation: reuse if valid, cleanup if incomplete
  EXTRACTION_SUCCESS=false

  if [[ -d "$WORK_DIR/BOOT-INF/classes" ]]; then
    echo "[STARTUP] Reusing valid previous extraction from $WORK_DIR"
    EXTRACTION_SUCCESS=true
  else
    # Directory missing or extraction incomplete - cleanup and re-extract
    if [[ -d "$WORK_DIR" ]]; then
      echo "[WARN] Incomplete extraction detected (missing BOOT-INF/classes). Cleaning up..."
      rm -rf "$WORK_DIR" || true
    fi

    echo "[STARTUP] Extracting WAR to tmpfs: $WORK_DIR"
    START_EXTRACT=$(date +%s%3N)

    mkdir -p "$WORK_DIR"

    # Extract WAR with fallback to normal startup if extraction fails
    if unzip -q -o -d "$WORK_DIR" "$JAR_PATH" 2>/dev/null; then
      END_EXTRACT=$(date +%s%3N)
      EXTRACT_TIME=$((END_EXTRACT - START_EXTRACT))
      echo "[STARTUP] WAR extracted in ${EXTRACT_TIME}ms"

      # Mark extraction as complete only after successful unzip
      touch "$EXTRACTION_COMPLETE" || { echo "[ERROR] Failed to mark extraction complete"; exit 1; }

      EXTRACTION_SUCCESS=true
    else
      echo "[WARN] WAR extraction failed. Falling back to normal startup (slower)..."
      rm -rf "$WORK_DIR" || true
      # Disable extraction optimization, use normal WAR startup
      JAR_EXTRACTION_OPTS="-jar /datahub/datahub-gms/bin/war.war"
    fi
  fi

  # Process classpath only if extraction succeeded
  if [[ "$EXTRACTION_SUCCESS" == true ]]; then
    echo "[STARTUP] Generating deterministic classpath from BOOT-INF/classpath.idx"

    IDX="$WORK_DIR/BOOT-INF/classpath.idx"
    if [[ ! -f "$IDX" ]]; then
      echo "[ERROR] Missing $IDX (this WAR may not be a Spring Boot executable archive)"
      exit 1
    fi

    # 1) Convert classpath.idx to absolute paths (one per line)
    #    Format: - "BOOT-INF/lib/foo.jar" → /tmp/gms-work/BOOT-INF/lib/foo.jar
    sed -E 's|^- "([^"]+)"$|'"$WORK_DIR"'/\1|' "$IDX" > "$WORK_DIR/classpath.paths"

    # 2) Prepend application classes (must come before library JARs)
    {
      printf "%s\n" "$WORK_DIR/BOOT-INF/classes"
      cat "$WORK_DIR/classpath.paths"
    } > "$WORK_DIR/classpath.all"

    # 3) Join into single classpath string (colon-separated) - write to file to avoid shell variable limits
    paste -sd: "$WORK_DIR/classpath.all" > "$WORK_DIR/classpath.joined"

    # 4) Create Java argfile (argfiles are for launcher args, not -cp values)
    ARGS_FILE="$WORK_DIR/java.args"
    cat > "$ARGS_FILE" <<EOF
-cp
$(cat "$WORK_DIR/classpath.joined")
com.linkedin.gms.GMSApplication
EOF

    ENTRY_COUNT=$(wc -l < "$WORK_DIR/classpath.all")
    echo "[STARTUP] Deterministic classpath: $ENTRY_COUNT entries (from classpath.idx)"
    echo "[STARTUP] This fixes class loading order but not version conflicts - ensure build uses consistent dependency versions"

    # Run with Java argfile (deterministic order, not filesystem-dependent)
    JAR_EXTRACTION_OPTS="@$ARGS_FILE"
  fi
else
  # Traditional WAR execution (nested JAR reads - slower)
  JAR_EXTRACTION_OPTS="-jar /datahub/datahub-gms/bin/war.war"
fi

COMMON="
    $WAIT_FOR_EBEAN \
    $WAIT_FOR_CASSANDRA \
    $WAIT_FOR_KAFKA \
    $WAIT_FOR_NEO4J \
    -timeout 240s \
    java $JAVA_OPTS $JMX_OPTS \
    $SPRING_PROFILE_OPTS \
    $OTEL_AGENT \
    $PROMETHEUS_AGENT \
    -Dstats=unsecure \
    $JAR_EXTRACTION_OPTS"

if [[ $SKIP_ELASTICSEARCH_CHECK != true ]]; then
  exec dockerize \
    -wait $ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT -wait-http-header "$ELASTICSEARCH_AUTH_HEADER" \
    $COMMON
else
  exec dockerize $COMMON
fi