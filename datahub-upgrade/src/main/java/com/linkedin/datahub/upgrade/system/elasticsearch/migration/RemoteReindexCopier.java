package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.search.RemoteCopySettings;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Submits the target cluster's own {@code _reindex} with a remote source and polls its task. The
 * bytes never pass through this process, but the source fetch is made by the target's nodes, so it
 * can only authenticate the way the engine allows (basic auth, or IAM pass-through on AWS
 * public-endpoint pairs) and self-managed targets must allowlist the source. The task id is the
 * resume cursor: a later run re-attaches to a task that is still running, and a task the cluster
 * has forgotten is simply re-run, which {@code op_type=create} makes safe.
 */
@Slf4j
public final class RemoteReindexCopier implements CrossClusterCopier {
  public static final String NAME = "remoteReindex";
  public static final String OPAQUE_ID_HEADER = "X-Opaque-Id";
  // Polling backs off from a quarter second to five seconds: small timeseries indices and the
  // window pass finish in well under a second, and a fixed five-second poll would make every one
  // of hundreds of such tasks take five seconds.
  private static final long FIRST_POLL_INTERVAL_MS = 250L;
  private static final long MAX_POLL_INTERVAL_MS = 5_000L;

  /** The source as the target's nodes must reach it. */
  public record Source(
      @Nonnull String host,
      @Nullable String username,
      @Nullable String password,
      boolean external,
      @Nonnull String socketTimeout,
      @Nonnull String connectTimeout) {

    @Nonnull
    public static Source of(
        @Nonnull final String host,
        @Nullable final String fallbackUsername,
        @Nullable final String fallbackPassword,
        @Nonnull final RemoteCopySettings settings) {
      return new Source(
          host,
          settings.getUsername() != null ? settings.getUsername() : fallbackUsername,
          settings.getPassword() != null ? settings.getPassword() : fallbackPassword,
          settings.isExternal(),
          settings.getSocketTimeout() == null ? "5m" : settings.getSocketTimeout(),
          settings.getConnectTimeout() == null ? "30s" : settings.getConnectTimeout());
    }
  }

  private final ClusterHttp target;
  private final Source source;
  private final String opaqueIdPrefix;
  private final int batchSize;
  private final int requestsPerSecond;

  /**
   * @param opaqueIdPrefix identifies this migration's tasks on the target; the partition id is
   *     appended
   */
  public RemoteReindexCopier(
      @Nonnull final ClusterHttp target,
      @Nonnull final Source source,
      @Nonnull final String opaqueIdPrefix,
      final int batchSize,
      final int requestsPerSecond) {
    this.target = target;
    this.source = source;
    this.opaqueIdPrefix = opaqueIdPrefix;
    this.batchSize = Math.max(1, batchSize);
    this.requestsPerSecond = requestsPerSecond;
  }

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public Result copy(
      @Nonnull final CopyPartition partition,
      @Nullable final Checkpoint resumeFrom,
      @Nonnull final Checkpointer checkpointer,
      @Nonnull final BooleanSupplier shouldStop)
      throws IOException, InterruptedException {
    if (partition.guardField() != null) {
      // A reindex script sees the source document only, never the target's.
      throw new IllegalArgumentException(
          "Remote reindex cannot replace documents guarded by "
              + partition.guardField()
              + "; "
              + partition.label()
              + " must copy in process");
    }
    final String opaqueId = opaqueId(partition);
    String taskId = resumeFrom == null ? null : resumeFrom.cursor();
    if (taskId != null && target.task(taskId) == null) {
      log.info("Task {} for {} is gone; looking for a running task by opaque id", taskId, opaqueId);
      taskId = null;
    }
    if (taskId == null) {
      taskId = findRunningTask(opaqueId);
    }
    if (taskId == null) {
      taskId = submit(partition, opaqueId);
      checkpointer.checkpoint(new Checkpoint(taskId, 0, 0, 0, 0));
    }
    long pollIntervalMs = FIRST_POLL_INTERVAL_MS;
    while (true) {
      final JsonNode task = target.task(taskId);
      if (task == null) {
        // Completed and already forgotten, or lost to a restart: the next attempt re-runs it.
        throw new IOException(
            "Remote reindex task " + taskId + " for " + partition.label() + " is no longer known");
      }
      if (task.path("completed").asBoolean()) {
        return result(task, partition);
      }
      if (shouldStop.getAsBoolean()) {
        log.info("Leaving remote reindex task {} ({}) running", taskId, partition.label());
        final JsonNode status = task.path("task").path("status");
        return new Result(
            status.path("total").asLong(),
            status.path("created").asLong() + status.path("version_conflicts").asLong(),
            status.path("created").asLong(),
            status.path("version_conflicts").asLong(),
            0,
            true);
      }
      Thread.sleep(pollIntervalMs);
      pollIntervalMs = Math.min(pollIntervalMs * 2, MAX_POLL_INTERVAL_MS);
    }
  }

  @Nonnull
  String opaqueId(@Nonnull final CopyPartition partition) {
    return opaqueIdPrefix + "|" + partition.id();
  }

  @Nonnull
  private String submit(@Nonnull final CopyPartition partition, @Nonnull final String opaqueId)
      throws IOException {
    final ObjectNode body = target.object();
    body.put("conflicts", "proceed");
    final ObjectNode sourceNode = body.putObject("source");
    final ObjectNode remote = sourceNode.putObject("remote");
    remote.put("host", source.host());
    if (source.username() != null && source.password() != null) {
      remote.put("username", source.username());
      remote.put("password", source.password());
    }
    if (source.external()) {
      remote.put("external", true);
    }
    remote.put("socket_timeout", source.socketTimeout());
    remote.put("connect_timeout", source.connectTimeout());
    sourceNode.put("index", partition.sourceIndex());
    sourceNode.put("size", batchSize);
    if (partition.filter() != null) {
      sourceNode.set("query", partition.filter());
    }
    body.putObject("dest").put("index", partition.targetIndex()).put("op_type", "create");
    final String endpoint =
        "/_reindex?wait_for_completion=false&requests_per_second=" + requestsPerSecond;
    final JsonNode response =
        target.jsonWithHeader("POST", endpoint, body, OPAQUE_ID_HEADER, opaqueId);
    final String taskId = response.path("task").asText();
    if (taskId.isEmpty()) {
      throw new IOException("Remote reindex for " + partition.label() + " returned no task id");
    }
    log.info("Submitted remote reindex task {} for {} as {}", taskId, partition.label(), opaqueId);
    return taskId;
  }

  @Nullable
  private String findRunningTask(@Nonnull final String opaqueId) throws IOException {
    final JsonNode tasks = target.runningTasks("*reindex");
    for (JsonNode node : tasks.path("nodes")) {
      final JsonNode nodeTasks = node.path("tasks");
      final Iterator<Map.Entry<String, JsonNode>> entries = nodeTasks.fields();
      while (entries.hasNext()) {
        final Map.Entry<String, JsonNode> entry = entries.next();
        if (opaqueId.equals(entry.getValue().path("headers").path(OPAQUE_ID_HEADER).asText())) {
          log.info("Re-attached to running remote reindex task {} ({})", entry.getKey(), opaqueId);
          return entry.getKey();
        }
      }
    }
    return null;
  }

  @Nonnull
  private Result result(@Nonnull final JsonNode task, @Nonnull final CopyPartition partition)
      throws IOException {
    if (task.has("error")) {
      throw new IOException(
          "Remote reindex for " + partition.label() + " failed: " + task.path("error"));
    }
    final JsonNode response = task.path("response");
    final long total = response.path("total").asLong();
    final long created = response.path("created").asLong();
    final long conflicts = response.path("version_conflicts").asLong();
    final long noops = response.path("noops").asLong();
    final long failures = response.path("failures").size();
    if (failures > 0) {
      log.error(
          "Remote reindex for {} reported {} failures; first: {}",
          partition.label(),
          failures,
          response.path("failures").get(0));
    }
    // The task's own identity: every document it read was created, already present, or a noop.
    final long read = created + conflicts + noops;
    return new Result(
        total, read, created + noops, conflicts, failures + Math.max(0, total - read), false);
  }
}
