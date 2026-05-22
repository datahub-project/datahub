package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.common.urn.Urn;
import java.util.List;
import lombok.Value;

/**
 * Result of enumerating members of a Domain or DataProduct. {@code total} is the unclamped count
 * (used to set {@link AggregatedLineageResponse#isPartial()} when greater than {@link
 * AggregatedLineageRequest#getMemberScanCap()}); {@code urns} is the list actually returned (≤
 * memberScanCap).
 */
@Value
public class MembersResult {
  List<Urn> urns;
  int total;
}
