package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.AspectSpec;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class MappingsBuilder {

  private MappingsBuilder() {
  }

  public static Map<String, Object> getMappings(@Nonnull final AspectSpec aspectSpec) {
    if (!aspectSpec.isTimeseries()) {
      throw new IllegalArgumentException(
          String.format("Cannot apply temporal stats indexing for a non-temporal aspect %s", aspectSpec.getName()));
    }

    Map<String, Object> mappings = new HashMap<>(getTemporalInfoMappings());

    mappings.put("urn", ImmutableMap.of("type", "keyword"));

    aspectSpec.getTemporalStatFieldSpecs()
        .stream()
        .map(MappingsBuilder::getTemporalStatMappings)
        .forEach(mappings::putAll);

    aspectSpec.getTemporalStatCollectionFieldSpecs()
        .stream()
        .map(MappingsBuilder::getTemporalStatCollectionMappings)
        .forEach(mappings::putAll);

    return mappings;
  }

  private static Map<String, Object> getTemporalInfoMappings() {
    Map<String, Object> temporalInfoMappings = new HashMap<>();
    temporalInfoMappings.put("@timestamp", ImmutableMap.of("type", "date"));
    temporalInfoMappings.put("eventTimestampMillis", ImmutableMap.of("type", "date"));
    temporalInfoMappings.put("eventGranularityMillis", ImmutableMap.of("type", "long"));

    Map<String, Object> partitionSpecMappings = new HashMap<>();
    partitionSpecMappings.put("partition", ImmutableMap.of("type", "keyword"));
    partitionSpecMappings.put("startTimeMillis", ImmutableMap.of("type", "date"));
    partitionSpecMappings.put("durationMillis", ImmutableMap.of("type", "long"));
    temporalInfoMappings.put("partitionSpec", partitionSpecMappings);

    return temporalInfoMappings;
  }

  private static Map<String, Object> getTemporalStatMappings(TemporalStatFieldSpec temporalStatFieldSpec) {
    Map<String, Object> mappings = new HashMap<>();
    String type;
    switch (temporalStatFieldSpec.getPegasusSchema().getType()) {
      case INT:
      case LONG:
        type = "long";
        break;
      case FLOAT:
      case DOUBLE:
        type = "double";
        break;
      default:
        type = "keyword";
        break;
    }
    mappings.put(temporalStatFieldSpec.getTemporalStatAnnotation().getStatName(), ImmutableMap.of("type", type));
    return mappings;
  }

  private static Map<String, Object> getTemporalStatCollectionMappings(
      TemporalStatCollectionFieldSpec temporalStatCollectionFieldSpec) {
    Map<String, Object> statsMappings = new HashMap<>();
    temporalStatCollectionFieldSpec.getTemporalStats()
        .stream()
        .map(MappingsBuilder::getTemporalStatMappings)
        .forEach(statsMappings::putAll);
    statsMappings.put("key", ImmutableMap.of("type", "keyword"));
    return ImmutableMap.of(temporalStatCollectionFieldSpec.getName(), statsMappings);
  }
}
