package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

public class MappingsBuilder {

  public static final String URN_FIELD = "urn";
  public static final String MESSAGE_ID_FIELD = "messageId";
  public static final String TIMESTAMP_FIELD = "@timestamp";
  public static final String TIMESTAMP_MILLIS_FIELD = "timestampMillis";
  public static final String EVENT_GRANULARITY = "eventGranularity";
  public static final String EVENT_FIELD = "event";
  public static final String SYSTEM_METADATA_FIELD = "systemMetadata";
  public static final String IS_EXPLODED_FIELD = "isExploded";
  public static final String PARTITION_SPEC = "partitionSpec";
  public static final String PARTITION_SPEC_PARTITION = "partition";
  public static final String PARTITION_SPEC_TIME_PARTITION = "timePartition";
  public static final String RUN_ID_FIELD = "runId";

  private MappingsBuilder() {}

  public static Map<String, Object> getMappings(@Nonnull final AspectSpec aspectSpec) {
    if (!aspectSpec.isTimeseries()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot apply timeseries field indexing for a non-timeseries aspect %s",
              aspectSpec.getName()));
    }

    Map<String, Object> mappings = new HashMap<>();

    mappings.put(RUN_ID_FIELD, ImmutableMap.of("type", "keyword"));
    mappings.put(URN_FIELD, ImmutableMap.of("type", "keyword"));
    mappings.put(MESSAGE_ID_FIELD, ImmutableMap.of("type", "keyword"));
    mappings.put(TIMESTAMP_FIELD, ImmutableMap.of("type", "date"));
    mappings.put(TIMESTAMP_MILLIS_FIELD, ImmutableMap.of("type", "date"));
    mappings.put(EVENT_GRANULARITY, ImmutableMap.of("type", "keyword"));
    mappings.put(
        PARTITION_SPEC,
        ImmutableMap.of(
            "properties",
            ImmutableMap.of(
                PARTITION_SPEC_PARTITION,
                ImmutableMap.of("type", "keyword"),
                PARTITION_SPEC_TIME_PARTITION,
                ImmutableMap.of("type", "keyword"))));
    mappings.put(EVENT_FIELD, ImmutableMap.of("type", "object", "enabled", false));
    mappings.put(SYSTEM_METADATA_FIELD, ImmutableMap.of("type", "object", "enabled", false));
    mappings.put(IS_EXPLODED_FIELD, ImmutableMap.of("type", "boolean"));

    aspectSpec
        .getTimeseriesFieldSpecs()
        .forEach(x -> mappings.put(x.getName(), getFieldMapping(x.getPegasusSchema().getType())));
    aspectSpec
        .getTimeseriesFieldCollectionSpecs()
        .forEach(x -> mappings.put(x.getName(), getTimeseriesFieldCollectionSpecMapping(x)));

    return ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getTimeseriesFieldCollectionSpecMapping(
      TimeseriesFieldCollectionSpec timeseriesFieldCollectionSpec) {
    Map<String, Object> collectionMappings = new HashMap<>();
    collectionMappings.put(
        timeseriesFieldCollectionSpec.getTimeseriesFieldCollectionAnnotation().getKey(),
        getFieldMapping(DataSchema.Type.STRING));
    timeseriesFieldCollectionSpec
        .getTimeseriesFieldSpecMap()
        .values()
        .forEach(
            x ->
                collectionMappings.put(
                    x.getName(), getFieldMapping(x.getPegasusSchema().getType())));
    return ImmutableMap.of("properties", collectionMappings);
  }

  private static Map<String, Object> getFieldMapping(DataSchema.Type dataSchemaType) {
    switch (dataSchemaType) {
      case INT:
        return ImmutableMap.of("type", "integer");
      case LONG:
        return ImmutableMap.of("type", "long");
      case FLOAT:
        return ImmutableMap.of("type", "float");
      case DOUBLE:
        return ImmutableMap.of("type", "double");
      default:
        return ImmutableMap.of("type", "keyword");
    }
  }
}
