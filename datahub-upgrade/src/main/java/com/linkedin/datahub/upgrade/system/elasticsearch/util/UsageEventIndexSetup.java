package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.Request;

/**
 * The usage event layout on one cluster: an ISM policy, an index template and a rollover alias over
 * {@code -000001} on OpenSearch; an ILM policy, an index template and a data stream on
 * Elasticsearch. {@code CreateUsageEventIndicesStep} builds it on the serving cluster around the
 * legacy-index migration; a family migration builds it on the target cluster, which has no legacy
 * index to migrate.
 */
@Slf4j
public final class UsageEventIndexSetup {
  private UsageEventIndexSetup() {}

  /** The names the layout uses under {@code prefix} (already ending in {@code _} or empty). */
  public record Names(
      String prefix, String policy, String template, String writeTarget, String firstIndex) {
    @Nonnull
    public static Names of(@Nonnull final String prefix) {
      return new Names(
          prefix,
          prefix + "datahub_usage_event_policy",
          prefix + "datahub_usage_event_index_template",
          prefix + "datahub_usage_event",
          prefix + "datahub_usage_event-000001");
    }
  }

  /**
   * Creates the policy and the index template.
   *
   * @return whether the write target may be created: always on Elasticsearch; on OpenSearch only
   *     when the ISM policy exists, since an index under the template without its policy would
   *     never roll over
   */
  public static boolean createPolicyAndTemplate(
      @Nonnull final OperationContext opContext,
      @Nonnull final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      final boolean openSearch,
      @Nonnull final Names names,
      final int numShards,
      final int numReplicas,
      final int usageRefreshSeconds)
      throws Exception {
    final String refreshInterval = usageRefreshSeconds + "s";
    if (openSearch) {
      final boolean policyCreated =
          UsageEventIndexUtils.createIsmPolicy(cluster, names.policy(), names.prefix(), opContext);
      log.info("ISM policy creation result: {}", policyCreated);
      if (!policyCreated) {
        log.warn(
            "ISM policy creation failed or is not supported. Skipping template and index creation"
                + " to avoid configuration issues.");
        return false;
      }
      log.info("Creating index template: {}", names.template());
      UsageEventIndexUtils.createOpenSearchIndexTemplate(
          opContext,
          cluster,
          names.template(),
          numShards,
          numReplicas,
          names.prefix(),
          refreshInterval);
      return true;
    }
    UsageEventIndexUtils.createIlmPolicy(opContext, cluster, names.policy());
    UsageEventIndexUtils.createIndexTemplate(
        opContext,
        cluster,
        names.template(),
        names.policy(),
        numShards,
        numReplicas,
        names.prefix(),
        refreshInterval);
    return true;
  }

  /** The alias over its first backing index (OpenSearch) or the data stream (Elasticsearch). */
  public static void createWriteTarget(
      @Nonnull final OperationContext opContext,
      @Nonnull final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      final boolean openSearch,
      @Nonnull final Names names)
      throws Exception {
    if (openSearch) {
      log.info(
          "Creating initial index: {} with alias: {}", names.firstIndex(), names.writeTarget());
      UsageEventIndexUtils.createOpenSearchUsageEventIndex(
          opContext, cluster, names.firstIndex(), names.writeTarget());
    } else {
      UsageEventIndexUtils.createDataStream(opContext, cluster, names.writeTarget());
    }
  }

  /**
   * The complete layout on a cluster with no legacy usage index, which is what a migration target
   * is. Idempotent: every piece tolerates already existing. A refused policy fails it outright: the
   * serving cluster's setup can carry on without one, but a target without the layout could never
   * adopt a copy, and finding that out at adoption time would waste the copy.
   */
  public static void createFresh(
      @Nonnull final OperationContext opContext,
      @Nonnull final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      @Nonnull final Names names,
      final int numShards,
      final int numReplicas,
      final int usageRefreshSeconds)
      throws Exception {
    final boolean openSearch = cluster.getSearchClient().getEngineType().isOpenSearch();
    if (!createPolicyAndTemplate(
        opContext, cluster, openSearch, names, numShards, numReplicas, usageRefreshSeconds)) {
      throw new IllegalStateException(
          "The usage event layout could not be created: the lifecycle policy "
              + names.policy()
              + " was refused, so no copy could be adopted into "
              + names.writeTarget());
    }
    createWriteTarget(opContext, cluster, openSearch, names);
    UsageEventIndexUtils.applyRefreshInterval(
        opContext, cluster, names.writeTarget(), usageRefreshSeconds);
  }

  /**
   * A concrete index carrying the usage event mappings but none of the lifecycle settings, for a
   * migration to copy into and adopt later. Named outside the template patterns on purpose, so
   * neither ISM nor the data-stream template claims it. Tolerates already existing.
   */
  public static void createStagingIndex(
      @Nonnull final OperationContext opContext,
      @Nonnull final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      final boolean openSearch,
      @Nonnull final String stagingIndex,
      final int numShards,
      final int numReplicas,
      final int usageRefreshSeconds)
      throws Exception {
    final String resource =
        openSearch
            ? "/index/usage-event/opensearch_template.json"
            : "/index/usage-event/elasticsearch_template.json";
    final String templateJson =
        IndexUtils.loadResourceAsString(resource)
            .replace("DUE_SHARDS", String.valueOf(numShards))
            .replace("DUE_REPLICAS", String.valueOf(numReplicas))
            .replace("DUE_REFRESH_INTERVAL", usageRefreshSeconds + "s")
            .replace("PREFIX", "");
    final JsonNode template = opContext.getObjectMapper().readTree(templateJson).path("template");
    final ObjectNode body = opContext.getObjectMapper().createObjectNode();
    body.set("mappings", template.path("mappings"));
    final ObjectNode settings = body.putObject("settings");
    settings.put("index.number_of_shards", numShards);
    settings.put("index.number_of_replicas", numReplicas);
    settings.put("index.refresh_interval", usageRefreshSeconds + "s");
    final Request request = new Request("PUT", "/" + stagingIndex);
    request.setJsonEntity(opContext.getObjectMapper().writeValueAsString(body));
    request.addParameter("ignore", "400");
    final RawResponse response =
        cluster.getSearchClient().performLowLevelRequest(opContext, request);
    final int status = response.getStatusLine().getStatusCode();
    if (status == 400) {
      final String bodyText =
          new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
      if (!bodyText.contains("resource_already_exists_exception")) {
        throw new IOException("Could not create staging index " + stagingIndex + ": " + bodyText);
      }
      log.info("Staging index {} already exists", stagingIndex);
      return;
    }
    log.info("Created usage staging index {}", stagingIndex);
  }
}
