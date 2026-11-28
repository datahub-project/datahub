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
 * <p>Both fields are optional (mirroring {@link PathSpec}). The env is validated at config-load
 * time (see {@code SparkConfigParser.getConnectionInstanceMap}) so an invalid env is reported once,
 * loudly, at the source instead of being silently dropped per dataset during URN construction.
 */
@Builder
@Getter
@ToString
public class ConnectionInstanceDetail {
  @Builder.Default final Optional<String> platformInstance = Optional.empty();

  /** A FabricType value or a registered custom environment. */
  @Builder.Default final Optional<String> envName = Optional.empty();

  /**
   * The environment as a FabricType, as before this class held a String. A custom environment reads
   * as {@code $UNKNOWN}; use {@link #getEnvName()} for the actual value.
   */
  public Optional<FabricType> getEnv() {
    return envName.map(
        name -> {
          try {
            return FabricType.valueOf(name);
          } catch (IllegalArgumentException e) {
            return FabricType.$UNKNOWN;
          }
        });
  }

  public static class ConnectionInstanceDetailBuilder {
    /** Sets a built-in environment; kept so existing callers compile unchanged. */
    public ConnectionInstanceDetailBuilder env(Optional<FabricType> env) {
      return envName(env.map(FabricType::name));
    }
  }
}
