package io.datahubproject.metadata.context.request;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.List;
import javax.annotation.concurrent.Immutable;
import lombok.Value;

/**
 * Immutable rules for parsing {@code X-DataHub-Context} into bounded metric/trace dimensions.
 *
 * <p><b>Production:</b> GMS builds an instance from {@code datahub.requestContext.contextHeader} in
 * {@code application.yaml} via {@code DataHubContextPolicyFactory.from} and installs it with {@link
 * DataHubContextRulesHolder#setPolicy}.
 *
 * <p>This package ({@code io.datahubproject.metadata.context.request}) has no Spring/YAML
 * dependency. {@link #defaults()} mirrors the <em>stock</em> YAML shape ({@code skill} / {@code
 * caller}, unrestricted allowlists) for JVM bootstrap before Spring, missing config, empty or
 * invalid {@code valueAllowlistsJson}, defensive paths in {@link DataHubContextParser}, and unit
 * tests—not as a second source of configurable defaults.
 */
@Value
@Immutable
public class DataHubContextParsePolicy {

  public static final String DEFAULT_OTHER_LABEL = "other";

  String unspecifiedLabel;
  String otherLabel;
  int maxValueLength;
  List<DataHubContextKeyRule> rules;

  /**
   * Stock policy matching an empty allowlist for {@code skill} and {@code caller} (same keys/tags
   * as typical {@code application.yaml}). Prefer YAML-backed policy from the factory when running
   * GMS.
   */
  public static DataHubContextParsePolicy defaults() {
    return new DataHubContextParsePolicy(
        DataHubContextParser.UNSPECIFIED,
        DEFAULT_OTHER_LABEL,
        DataHubContextParser.DEFAULT_MAX_VALUE_LENGTH,
        List.of(
            new DataHubContextKeyRule("caller", MetricUtils.TAG_AGENT_CALLER, java.util.Set.of()),
            new DataHubContextKeyRule("skill", MetricUtils.TAG_AGENT_SKILL, java.util.Set.of())));
  }
}
