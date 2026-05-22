package com.linkedin.datahub.graphql.resolvers.lineage.aggregate;

import com.linkedin.common.urn.Urn;
import java.util.List;
import lombok.Value;

/**
 * Member enumeration result. {@code total} is the unclamped count; {@code urns} is what we actually
 * return (≤ memberScanCap). Caller flips {@code isPartial} when {@code total > urns.size()}.
 */
@Value
public class MembersResult {
  List<Urn> urns;
  int total;
}
