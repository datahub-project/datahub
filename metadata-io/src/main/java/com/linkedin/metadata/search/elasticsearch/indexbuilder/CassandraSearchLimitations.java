package com.linkedin.metadata.search.elasticsearch.indexbuilder;

/**
 * Limits that apply when Cassandra is the primary storage ({@code entityService.impl=cassandra})
 * and a search path that reads primary storage is on. Each startup warning names all three, so an
 * operator who sees one of them sees the others.
 */
public final class CassandraSearchLimitations {

  public static final String ALL =
      "Limits with entityService.impl=cassandra: Search V3 does not copy existing rows, later"
          + " writes fill the index, and while V2 indices exist startup keeps search on V2."
          + " Incremental reindex cannot stream Cassandra aspects, so a run that needs a fill"
          + " fails; keep ZDU_STAGE_20 and incremental reindex off. RestoreIndices runs no steps"
          + " here: it reads no rows and does not clear the index, including with -a clean.";

  private CassandraSearchLimitations() {}
}
