package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import com.linkedin.metadata.models.TimeseriesFieldSpec;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class MappingsBuilder {

  public static final String URN_FIELD = "urn";
  public static final String TIMESTAMP_FIELD = "@timestamp";
  public static final String TIMESTAMP_MILLIS_FIELD = "timestampMillis";
  public static final String EVENT_GRANULARITY = "eventGranularity";
  public static final String EVENT_FIELD = "event";
  public static final String SYSTEM_METADATA_FIELD = "systemMetadata";
  public static final String IS_EXPLODED_FIELD = "isExploded";

  private MappingsBuilder() {
  }

  public static Map<String, Object> getMappings(@Nonnull final AspectSpec aspectSpec) {
    if (!aspectSpec.isTimeseries()) {
      throw new IllegalArgumentException(
          String.format("Cannot apply timeseries field indexing for a non-timeseries aspect %s", aspectSpec.getName()));
    }

    Map<String, Object> mappings = new HashMap<>();

    mappings.put(URN_FIELD, ImmutableMap.of("type", "keyword"));
    mappings.put(TIMESTAMP_FIELD, ImmutableMap.of("type", "date"));
    mappings.put(TIMESTAMP_MILLIS_FIELD, ImmutableMap.of("type", "date"));
    mappings.put(EVENT_GRANULARITY, ImmutableMap.of("type", "keyword"));
    mappings.put(EVENT_FIELD, ImmutableMap.of("type", "object", "enabled", false));
    mappings.put(SYSTEM_METADATA_FIELD, ImmutableMap.of("type", "object", "enabled", false));
    mappings.put(IS_EXPLODED_FIELD, ImmutableMap.of("type", "boolean"));
    aspectSpec.getTimeseriesFieldSpecs().stream().forEach(x -> setTimeseriesFieldSpecMapping(x, mappings));
    aspectSpec.getTimeseriesFieldCollectionSpecs()
        .stream()
        .forEach(x -> setTimeseriesFieldCollectionSpecMapping(x, mappings));

    return ImmutableMap.of("properties", mappings);
  }

  private static <K, V> void setTimeseriesFieldSpecMapping(TimeseriesFieldSpec timeseriesFieldSpec,
      Map<String, Object> mappings) {
    mappings.put(timeseriesFieldSpec.getName(), getFieldMapping(timeseriesFieldSpec.getPegasusSchema().getType()));
  }

  private static <K, V> void setTimeseriesFieldCollectionSpecMapping(
      TimeseriesFieldCollectionSpec timeseriesFieldCollectionSpec, Map<String, Object> mappings) {
    String collectionFieldName = timeseriesFieldCollectionSpec.getName();
    timeseriesFieldCollectionSpec.getTimeseriesFieldSpecMap()
        .values()
        .forEach(x -> mappings.put(collectionFieldName + "." + x.getName(),
            getFieldMapping(x.getPegasusSchema().getType())));
    mappings.put(
        collectionFieldName + "." + timeseriesFieldCollectionSpec.getTimeseriesFieldCollectionAnnotation().getKey(),
        getFieldMapping(DataSchema.Type.STRING));
  }

  private static <K, V> ImmutableMap<K, V> getFieldMapping(DataSchema.Type dataSchemaType) {
    switch (dataSchemaType) {
      case INT:
        return (ImmutableMap<K, V>) ImmutableMap.of("type", "integer");
      case LONG:
        return (ImmutableMap<K, V>) ImmutableMap.of("type", "long");
      case FLOAT:
        return (ImmutableMap<K, V>) ImmutableMap.of("type", "float");
      case DOUBLE:
        return (ImmutableMap<K, V>) ImmutableMap.of("type", "double");
      case RECORD:
        return (ImmutableMap<K, V>) ImmutableMap.of("type", "object", "enabled", false);
      case STRING:
      default:
        return (ImmutableMap<K, V>) ImmutableMap.of("type", "keyword");
    }
  }
}
