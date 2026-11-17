package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

/**
 * How a family's live index is laid out on a cluster. The copy is written against the layout, not
 * the family: a rollover alias and a data stream both spread writes over backing indices, so both
 * are copied per backing index into concrete staging indices that are adopted at the end.
 */
public enum IndexLayout {
  /** One index, possibly behind a single-index alias left by an earlier in-place reindex. */
  CONCRETE,
  /** An alias whose write index rolls over (usage events on OpenSearch, ISM-managed). */
  ROLLOVER_ALIAS,
  /** A data stream (usage events on Elasticsearch, ILM-managed). */
  DATA_STREAM
}
