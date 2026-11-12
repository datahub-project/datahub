package datahub.spark;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Fluent, typed builder for the DataHub Spark listener configuration used by the real-Spark smoke
 * tests.
 *
 * <p>It exists to make each test's configuration legible. The typed methods name the knobs the
 * suite exercises, so the {@code spark.datahub.*} key strings live in exactly one place (a typo
 * can't silently no-op), {@link #set} is an escape hatch for anything not yet first-class, and
 * {@link #describe} renders a human-readable block showing which knobs are on/off, the default each
 * differs from, and the raw keys actually applied. {@code SparkSmokeTestBase#runJob} logs that
 * block before every job, so the test output shows exactly what configuration produced the result.
 */
final class ListenerConf {

  private final LinkedHashMap<String, String> raw = new LinkedHashMap<>();
  // Keys produced by a typed method, so describe() can separate them from raw set() escape-hatch
  // keys.
  private final Set<String> managedKeys = new LinkedHashSet<>();
  private final LinkedHashMap<String, String[]> connections = new LinkedHashMap<>();

  private Path emitFile;
  private Boolean captureColumnLevelLineage;
  private String globalPlatformInstance;
  private String globalEnv;

  static ListenerConf listener() {
    return new ListenerConf();
  }

  private ListenerConf put(String key, String value) {
    raw.put(key, value);
    managedKeys.add(key);
    return this;
  }

  /** Emit MCPs to a file; the test reads them back from this same path. */
  ListenerConf emitToFile(Path file) {
    this.emitFile = file;
    put("spark.datahub.emitter", "file");
    put("spark.datahub.file.filename", file.toString());
    return this;
  }

  ListenerConf captureColumnLevelLineage(boolean enabled) {
    this.captureColumnLevelLineage = enabled;
    return put("spark.datahub.metadata.dataset.captureColumnLevelLineage", String.valueOf(enabled));
  }

  /** The global platform_instance applied to datasets that have no per-connection mapping. */
  ListenerConf platformInstance(String instance) {
    this.globalPlatformInstance = instance;
    return put("spark.datahub.metadata.dataset.platformInstance", instance);
  }

  ListenerConf env(String env) {
    this.globalEnv = env;
    return put("spark.datahub.metadata.dataset.env", env);
  }

  /**
   * The platform_instance applied to the Spark pipeline's DataFlow/DataJob (not the datasets). It
   * qualifies the flow name, e.g. {@code (spark,<instance>.<app>,…)}.
   */
  ListenerConf pipelinePlatformInstance(String instance) {
    return put("spark.datahub.metadata.pipeline.platformInstance", instance);
  }

  /** Tags applied to the emitted DataFlow. */
  ListenerConf tags(String... tags) {
    return put("spark.datahub.tags", String.join(",", tags));
  }

  /** Domains applied to the emitted DataFlow. */
  ListenerConf domains(String... domains) {
    return put("spark.datahub.domains", String.join(",", domains));
  }

  /** Lower-case all emitted dataset URNs. */
  ListenerConf lowerCaseUrns(boolean enabled) {
    return put("spark.datahub.metadata.dataset.lowerCaseUrns", String.valueOf(enabled));
  }

  /** Emit standalone dataset entities (status/key) in addition to the lineage edges. */
  ListenerConf materialize(boolean enabled) {
    return put("spark.datahub.metadata.dataset.materialize", String.valueOf(enabled));
  }

  /**
   * Whether to coalesce all of the application's SQL executions into a single DataFlow/DataJob
   * emitted at app end (the default), versus emitting one DataJob per execution when disabled.
   */
  ListenerConf coalesce(boolean enabled) {
    return put("spark.datahub.coalesce_jobs", String.valueOf(enabled));
  }

  /**
   * A regex stripped (anchored at the end) from file dataset paths, e.g. {@code /dt=[^/]*} turns
   * {@code .../events/dt=2024-01-01} into {@code .../events}.
   */
  ListenerConf filePartitionRegexp(String regex) {
    return put("spark.datahub.file_partition_regexp", regex);
  }

  /**
   * Map matching file paths to a platform + dataset name via a path spec (e.g. platform {@code s3},
   * alias {@code my_tables}, glob {@code s3://bucket/{table}/*}). See {@code path_spec_list}.
   */
  ListenerConf pathSpec(String platform, String alias, String glob) {
    return put("spark.datahub." + platform + "." + alias + ".path_spec_list", glob);
  }

  /**
   * Map a connection (keyed by its OpenLineage namespace, e.g. {@code postgres://host:port}) to a
   * platform_instance and env. Repeatable for multi-source jobs.
   */
  ListenerConf connection(String namespace, String platformInstance, String env) {
    connections.put(namespace, new String[] {platformInstance, env});
    String prefix = "spark.datahub.metadata.dataset.connections.\"" + namespace + "\".";
    put(prefix + "platformInstance", platformInstance);
    if (env != null) {
      put(prefix + "env", env);
    }
    return this;
  }

  /** Escape hatch: set any {@code spark.datahub.*} (or other) key not covered by a typed method. */
  ListenerConf set(String key, String value) {
    raw.put(key, value);
    return this;
  }

  Map<String, String> toSparkConf() {
    return new LinkedHashMap<>(raw);
  }

  Path emitFile() {
    return emitFile;
  }

  /** A readable, multi-line summary of the effective configuration for test logs and failures. */
  String describe() {
    StringBuilder sb = new StringBuilder();
    sb.append("=== DataHub Spark listener config ===\n");
    sb.append(line("emitter", emitFile != null ? "file -> " + emitFile : "<none>"));
    sb.append(flag("captureColumnLevelLineage", captureColumnLevelLineage));
    sb.append(line("platformInstance (global)", value(globalPlatformInstance)));
    sb.append(line("env (global)", globalEnv != null ? globalEnv : "<unset> [default PROD]"));

    sb.append(String.format("  %-26s : ", "connections"));
    if (connections.isEmpty()) {
      sb.append("<none>\n");
    } else {
      sb.append('\n');
      connections.forEach(
          (ns, ie) ->
              sb.append("      ")
                  .append(ns)
                  .append("  ->  instance=")
                  .append(ie[0])
                  .append(", env=")
                  .append(ie[1] != null ? ie[1] : "<inherit>")
                  .append('\n'));
    }

    boolean headerWritten = false;
    for (Map.Entry<String, String> e : raw.entrySet()) {
      if (!managedKeys.contains(e.getKey())) {
        if (!headerWritten) {
          sb.append("  other (via set)            :\n");
          headerWritten = true;
        }
        sb.append("      ").append(e.getKey()).append(" = ").append(e.getValue()).append('\n');
      }
    }

    sb.append("--- raw spark.datahub.* applied ---\n");
    raw.forEach((k, v) -> sb.append("  ").append(k).append(" = ").append(v).append('\n'));
    return sb.toString();
  }

  private static String line(String label, String value) {
    return String.format("  %-26s : %s%n", label, value);
  }

  private static String flag(String label, Boolean state) {
    // All current boolean knobs default OFF when unset.
    String shown = Boolean.TRUE.equals(state) ? "ON" : "OFF";
    return String.format("  %-26s : %-7s [default OFF]%n", label, shown);
  }

  private static String value(String v) {
    return v != null ? v : "<unset>";
  }
}
