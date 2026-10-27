package com.linkedin.metadata.graph.lineage.core;

import java.util.List;

/**
 * An impact walk request. The limits mirror {@code elasticsearch.search.graph.impact.*} and are
 * sent by the caller, so the server needs no limit config of its own.
 *
 * @param startUrn the root of the walk
 * @param rules allowed edges, from {@code LineageGraphFilters.streamEdgeInfo()}
 * @param maxHops maximum number of hops
 * @param maxRelations stop after this many relationships; {@code <= 0} means unlimited
 * @param partialResults return a partial result on a limit or timeout instead of failing
 * @param startTimeMillis lineage time window start; accepted but ignored (no edge timestamps)
 * @param endTimeMillis lineage time window end; accepted but ignored
 * @param timeoutMillis walk time budget; {@code <= 0} means no deadline
 */
public record WalkRequest(
    String startUrn,
    List<EdgeFilter.Rule> rules,
    int maxHops,
    int maxRelations,
    boolean partialResults,
    Long startTimeMillis,
    Long endTimeMillis,
    long timeoutMillis) {}
