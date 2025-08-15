package datahub.spark.conf;

import com.typesafe.config.Config;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
@Setter
public class SparkLineageConf {
  final DatahubOpenlineageConfig openLineageConf;
  @Builder.Default final boolean coalesceEnabled = true;
  @Builder.Default final boolean emitCoalescePeriodically = false;
  @Builder.Default final boolean logMcps = true;
  final SparkAppContext sparkAppContext;
  final DatahubEmitterConfig datahubEmitterConfig;
  @Builder.Default final List<String> tags = new LinkedList<>();

  @Builder.Default final List<String> domains = new LinkedList<>();

  public static SparkLineageConf toSparkLineageConf(
      Config sparkConfig,
      SparkAppContext sparkAppContext,
      DatahubEmitterConfig datahubEmitterConfig) {
    SparkLineageConfBuilder builder = SparkLineageConf.builder();
    DatahubOpenlineageConfig datahubOpenlineageConfig =
        SparkConfigParser.sparkConfigToDatahubOpenlineageConf(sparkConfig, sparkAppContext);
    builder.openLineageConf(datahubOpenlineageConfig);
    builder.coalesceEnabled(SparkConfigParser.isCoalesceEnabled(sparkConfig));
    builder.logMcps(SparkConfigParser.isLogMcps(sparkConfig));
    if (SparkConfigParser.getTags(sparkConfig) != null) {
      builder.tags(Arrays.asList(Objects.requireNonNull(SparkConfigParser.getTags(sparkConfig))));
    }

    if (SparkConfigParser.getDomains(sparkConfig) != null) {
      builder.domains(
          Arrays.asList(Objects.requireNonNull(SparkConfigParser.getDomains(sparkConfig))));
    }

    builder.emitCoalescePeriodically(SparkConfigParser.isEmitCoalescePeriodically(sparkConfig));
    if (sparkAppContext != null) {
      builder.sparkAppContext(sparkAppContext);
    }

    if (datahubEmitterConfig != null) {
      builder.datahubEmitterConfig = datahubEmitterConfig;
    }
    return builder.build();
  }
}
