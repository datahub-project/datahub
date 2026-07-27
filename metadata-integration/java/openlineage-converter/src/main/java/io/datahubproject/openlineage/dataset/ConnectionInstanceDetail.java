package io.datahubproject.openlineage.dataset;

import com.linkedin.common.FabricType;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Resolves a connection identity (e.g. an AWS account id behind a Glue ARN) to the {@code
 * platform_instance} (and optionally {@code env}) that the owning platform's connector stamps, so
 * cross-account lineage URNs match. See the Glue ARN handling in {@code OpenLineageToDataHub}.
 *
 * <p>Both fields are optional (mirroring {@link PathSpec}). {@code env} is held as a validated
 * {@link FabricType} rather than a raw string: the value is parsed at config-load time (see {@code
 * SparkConfigParser.getConnectionInstanceMap}) so an invalid env is reported once, loudly, at the
 * source instead of being silently dropped per dataset during URN construction.
 */
@Builder
@Getter
@ToString
public class ConnectionInstanceDetail {
  @Builder.Default final Optional<String> platformInstance = Optional.empty();
  @Builder.Default final Optional<FabricType> env = Optional.empty();
}
