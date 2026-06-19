package io.datahubproject.openlineage.dataset;

import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Resolves a connection identity (e.g. an AWS account id behind a Glue ARN) to the {@code
 * platform_instance} (and optionally {@code env}) that the owning platform's connector stamps, so
 * cross-account lineage URNs match. See the Glue ARN handling in {@code OpenLineageToDataHub}.
 */
@Builder
@Getter
@ToString
public class ConnectionInstanceDetail {
  final String platformInstance;
  @Builder.Default final Optional<String> env = Optional.empty();
}
