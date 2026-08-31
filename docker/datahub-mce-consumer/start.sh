#!/bin/bash

source /usr/local/lib/datahub/wait_for_deps.sh

: ${SKIP_KAFKA_CHECK:=true}

OTEL_AGENT=""
if [[ $ENABLE_OTEL == true ]]; then
  OTEL_AGENT="-javaagent:opentelemetry-javaagent.jar "
fi

PROMETHEUS_AGENT=""
if [[ $ENABLE_PROMETHEUS == true ]]; then
  PROMETHEUS_AGENT="-javaagent:jmx_prometheus_javaagent.jar=4318:/datahub/datahub-mce-consumer/scripts/prometheus-config.yaml "
fi

export MANAGEMENT_SERVER_PORT="${MANAGEMENT_SERVER_PORT:-4319}"

# JAR extraction optimization - extract to tmpfs for faster class loading
JAR_EXTRACTION_OPTS=""
if [[ $EXTRACT_JAR_ENABLED == true ]]; then
  WORK_DIR="/tmp/mce/extraction"
  JAR_PATH="/datahub/datahub-mce-consumer/bin/mce-consumer-job.jar"

  # Log JAR size and available resources
  JAR_SIZE_MB=$(du -m "$JAR_PATH" | awk '{print $1}')
  AVAILABLE_RAM=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo | awk '{print int($1/1024)}')
  echo "[STARTUP] JAR extraction enabled. JAR size: ${JAR_SIZE_MB}MB, Available RAM: ${AVAILABLE_RAM}MB"

  if [[ $JAR_SIZE_MB -gt 1000 ]]; then
    echo "[WARN] JAR size (${JAR_SIZE_MB}MB) exceeds tmpfs limit (1Gi). Extraction may fail"
  fi

  if [[ $AVAILABLE_RAM -lt 500 ]]; then
    echo "[WARN] Low available RAM (${AVAILABLE_RAM}MB). Extraction may fail or trigger swap"
  fi

  # Always do fresh extraction (no reuse to avoid stale data on image updates)
  if [[ -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi

  echo "[STARTUP] Extracting JAR with Spring layertools to tmpfs: $WORK_DIR"
  START_EXTRACT=$(date +%s%3N)

  mkdir -p "$WORK_DIR"

  # Extract JAR with Spring layertools with fallback to normal startup if extraction fails
  if java -Djarmode=layertools -jar "$JAR_PATH" extract --destination "$WORK_DIR" 2>/dev/null; then
    END_EXTRACT=$(date +%s%3N)
    EXTRACT_TIME=$((END_EXTRACT - START_EXTRACT))
    echo "[STARTUP] JAR extracted in ${EXTRACT_TIME}ms"

    EXTRACTION_SUCCESS=true
  else
    echo "[WARN] JAR extraction failed. Falling back to normal startup (slower)..."
    # Disable extraction optimization, use normal JAR startup
    JAR_EXTRACTION_OPTS="-jar /datahub/datahub-mce-consumer/bin/mce-consumer-job.jar"
    EXTRACTION_SUCCESS=false
  fi

  # Process classpath only if extraction succeeded
  if [[ "$EXTRACTION_SUCCESS" == true ]]; then
    echo "[STARTUP] Generating deterministic classpath from extracted layers"

    ARGS_FILE="$WORK_DIR/java.args"

    # Build classpath from layers: application classes first, then dependencies

        # Build classpath from layers: application classes first, then dependencies
        # This is about class resolution priority in Java's classpath:

        # 1. $WORK_DIR/application/BOOT-INF/classes     ← Application code FIRST
        # 2. dependencies/BOOT-INF/lib                   ← Stable libraries
        # 3. snapshot-dependencies/BOOT-INF/lib          ← Dev/snapshot versions
        # 4. application/BOOT-INF/lib                    ← App-bundled libraries
        # 5. spring-boot-loader                          ← Infrastructure LAST

        #   Why this order matters:

        # 1. Application classes first — Java searches classpath left-to-right. By putting your app code first,
        # if there's a conflict, our version of a class wins (class shadowing).
        # 2. Stable deps next — Third-party libraries that rarely change, so this layer is cached well in Docker.
        # 3. Snapshot deps — Development/beta versions, change more often than stable.
        # 4. App-bundled libs — Custom or vendored dependencies specific to your app.
        # 5. Loader last — Spring Boot's classloader machinery goes last (infrastructure, not business logic).
    {
      printf "%s\n" "$WORK_DIR/application/BOOT-INF/classes"
      find "$WORK_DIR"/dependencies/BOOT-INF/lib -name "*.jar" -type f 2>/dev/null | sort || true
      find "$WORK_DIR"/snapshot-dependencies/BOOT-INF/lib -name "*.jar" -type f 2>/dev/null | sort || true
      find "$WORK_DIR"/application/BOOT-INF/lib -name "*.jar" -type f 2>/dev/null | sort || true
      printf "%s\n" "$WORK_DIR/spring-boot-loader"
    } > "$WORK_DIR/classpath.all"

    # Create colon-separated classpath
    paste -sd: "$WORK_DIR/classpath.all" > "$WORK_DIR/classpath.joined"

    # Create Java argfile
    cat > "$ARGS_FILE" <<EOF
-cp
$(cat "$WORK_DIR/classpath.joined")
com.linkedin.metadata.kafka.MceConsumerApplication
EOF

    ENTRY_COUNT=$(wc -l < "$WORK_DIR/classpath.all")
    echo "[STARTUP] Deterministic classpath: $ENTRY_COUNT entries (from extracted layers)"
    echo "[STARTUP] This fixes class loading order but not version conflicts - ensure build uses consistent dependency versions"

    # Run with Java argfile (deterministic order, not filesystem-dependent)
    JAR_EXTRACTION_OPTS="@$ARGS_FILE"
  fi
else
  # Traditional JAR execution (slower)
  JAR_EXTRACTION_OPTS="-jar /datahub/datahub-mce-consumer/bin/mce-consumer-job.jar"
fi

# Hazelcast 5.x on Java 9+ needs JPMS access for JDK internals (performance; avoids startup warning).
HAZELCAST_JVM_OPTS="--add-modules java.se --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"

datahub_wait_begin

if [[ $SKIP_KAFKA_CHECK != true ]]; then
  IFS=',' read -ra _kbs <<< "$KAFKA_BOOTSTRAP_SERVER"
  for _kb in "${_kbs[@]}"; do
    _kb="$(echo "$_kb" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -n "$_kb" ]] || continue
    [[ "$_kb" == tcp://* ]] || _kb="tcp://${_kb}"
    datahub_wait_tcp "$_kb"
  done
fi

if [[ "$KAFKA_SCHEMAREGISTRY_URL" && $SKIP_SCHEMA_REGISTRY_CHECK != true ]]; then
  datahub_wait_endpoint "$KAFKA_SCHEMAREGISTRY_URL"
fi

exec java $HAZELCAST_JVM_OPTS $JAVA_OPTS $JMX_OPTS $OTEL_AGENT $PROMETHEUS_AGENT $JAR_EXTRACTION_OPTS