package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.common.urn.Urn;
import java.util.List;
import lombok.Data;

@Data
public class DownstreamSummary {
  /** The urns of downstream assets. */
  List<Urn> assetUrns;

  /** The urns of downstream owners. */
  List<Urn> ownerUrns;

  /** The total number of downstream assets. */
  int total;
}
