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
   * @param finished true when nothing writes to the staging index any more; see {@link #settle}
   */
  public void adopt(
      @Nonnull final IndexLayout layout,
      @Nonnull final String logicalName,
      @Nonnull final String stagingIndex,
      final boolean finished)
      throws IOException {
    switch (layout) {
      case ROLLOVER_ALIAS:
        addToAlias(logicalName, stagingIndex);
        break;
      case DATA_STREAM:
        addToDataStream(logicalName, stagingIndex);
        break;
      default:
        throw new IllegalArgumentException("A concrete index is not adopted: " + stagingIndex);
    }
    settle(layout, stagingIndex, finished);
    log.info(
        "Adopted {} into {} {} as a historical member (finished={})",
        stagingIndex,
        layout,
        logicalName,
        finished);
  }

  /**
   * What a member needs once it is in, idempotent so a run interrupted between the join and this
   * completes it on a member it finds already in place. An alias member loses the ISM policy the
   * template attached (the policy's rollover action can only fail on a non-write index); a
   * data-stream member that nothing writes to any more is marked a completed generation, so ILM
   * treats it as rolled over.
   */
  public void settle(
      @Nonnull final IndexLayout layout, @Nonnull final String stagingIndex, final boolean finished)
      throws IOException {
    switch (layout) {
      case ROLLOVER_ALIAS:
        // A cluster without the plugin answers 400, 404 or 405 and has no policy to remove; a
        // refusal is an error.
        target.answer("POST", "/_plugins/_ism/remove/" + stagingIndex, null, "400", "404", "405");
        return;
      case DATA_STREAM:
        if (finished) {
          final ObjectNode settings = target.object();
          settings.put("index.lifecycle.indexing_complete", true);
          target.json("PUT", "/" + stagingIndex + "/_settings", settings, "400");
        }
        return;
      default:
        throw new IllegalArgumentException("A concrete index is not adopted: " + stagingIndex);
    }
  }

  private void addToAlias(@Nonnull final String alias, @Nonnull final String stagingIndex)
      throws IOException {
    final ObjectNode body = target.object();
    final ArrayNode actions = body.putArray("actions");
    final ObjectNode add = actions.addObject().putObject("add");
    add.put("index", stagingIndex);
    add.put("alias", alias);
    add.put("is_write_index", false);
    target.json("POST", "/_aliases", body);
  }

  private void addToDataStream(@Nonnull final String dataStream, @Nonnull final String stagingIndex)
      throws IOException {
    final ObjectNode body = target.object();
    final ObjectNode add = body.putArray("actions").addObject().putObject("add_backing_index");
    add.put("data_stream", dataStream);
    add.put("index", stagingIndex);
    target.json("POST", "/_data_stream/_modify", body);
  }
}
