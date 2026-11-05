package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Makes a copied staging index a historical generation of the target's rollover alias or data
 * stream. On OpenSearch the index joins the alias as a non-write member and loses the ISM policy
 * the template attached (the policy's rollover action would fail on a non-write index). On
 * Elasticsearch it joins the data stream through the modify API and, once nothing writes to it any
 * more, is marked as a finished generation for ILM. No member is ever write-blocked: the mirror
 * writes to a live staging index by name, before a flip and again after a rollback, and a write
 * addressed to the alias never lands on a non-write member anyway.
 */
@Slf4j
public final class StagingIndexAdopter {
  private final ClusterHttp target;

  public StagingIndexAdopter(@Nonnull final ClusterHttp target) {
    this.target = target;
  }

  /**
   * @param finished true when nothing writes to the staging index any more; see {@link #finish}
   */
  public void adopt(
      @Nonnull final IndexLayout layout,
      @Nonnull final String logicalName,
      @Nonnull final String stagingIndex,
      final boolean finished)
      throws IOException {
    switch (layout) {
      case ROLLOVER_ALIAS:
        adoptIntoAlias(logicalName, stagingIndex);
        break;
      case DATA_STREAM:
        adoptIntoDataStream(logicalName, stagingIndex);
        break;
      default:
        throw new IllegalArgumentException("A concrete index is not adopted: " + stagingIndex);
    }
    if (finished) {
      finish(layout, stagingIndex);
    }
    log.info(
        "Adopted {} into {} {} as a historical member (finished={})",
        stagingIndex,
        layout,
        logicalName,
        finished);
  }

  /**
   * Marks an adopted data-stream member that nothing writes to any more as a completed generation,
   * so ILM treats it as rolled over. An alias member needs nothing.
   */
  public void finish(@Nonnull final IndexLayout layout, @Nonnull final String stagingIndex)
      throws IOException {
    switch (layout) {
      case ROLLOVER_ALIAS:
        return;
      case DATA_STREAM:
        final ObjectNode settings = target.object();
        settings.put("index.lifecycle.indexing_complete", true);
        target.json("PUT", "/" + stagingIndex + "/_settings", settings, "400");
        return;
      default:
        throw new IllegalArgumentException("A concrete index is not adopted: " + stagingIndex);
    }
  }

  private void adoptIntoAlias(@Nonnull final String alias, @Nonnull final String stagingIndex)
      throws IOException {
    final ObjectNode body = target.object();
    final ArrayNode actions = body.putArray("actions");
    final ObjectNode add = actions.addObject().putObject("add");
    add.put("index", stagingIndex);
    add.put("alias", alias);
    add.put("is_write_index", false);
    target.json("POST", "/_aliases", body);
    // The template's ISM policy attaches to every index matching the pattern; on a non-write
    // member its rollover action can only fail. A cluster without the plugin answers 400, 404 or
    // 405 and has no policy to remove; a refusal is an error.
    target.answer("POST", "/_plugins/_ism/remove/" + stagingIndex, null, "400", "404", "405");
  }

  private void adoptIntoDataStream(
      @Nonnull final String dataStream, @Nonnull final String stagingIndex) throws IOException {
    final ObjectNode body = target.object();
    final ObjectNode add = body.putArray("actions").addObject().putObject("add_backing_index");
    add.put("data_stream", dataStream);
    add.put("index", stagingIndex);
    target.json("POST", "/_data_stream/_modify", body);
  }
}
