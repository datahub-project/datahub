package com.linkedin.metadata.timeseries.transformer;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import com.linkedin.metadata.models.TimeseriesFieldSpec;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;


/**
 * Class that provides a utility function that transforms the timeseries aspect into a document
 */
@Slf4j
public class TimeseriesAspectTransformer {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TimeseriesAspectTransformer() {
  }

  public static Map<String, JsonNode> transform(@Nonnull final Urn urn, @Nonnull final RecordTemplate timeseriesAspect,
      @Nonnull final AspectSpec aspectSpec, @Nullable final SystemMetadata systemMetadata)
      throws JsonProcessingException {
    ObjectNode commonDocument = getCommonDocument(urn, timeseriesAspect, systemMetadata);
    Map<String, JsonNode> finalDocuments = new HashMap<>();

    // NOTE: We keep the `event` and `systemMetadata` only with the aspect-level record.
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.setAll(commonDocument);
    document.set(MappingsBuilder.IS_EXPLODED_FIELD, JsonNodeFactory.instance.booleanNode(false));
    document.set(MappingsBuilder.EVENT_FIELD, OBJECT_MAPPER.readTree(RecordUtils.toJsonString(timeseriesAspect)));
    if (systemMetadata != null) {
      document.set(MappingsBuilder.SYSTEM_METADATA_FIELD,
          OBJECT_MAPPER.readTree(RecordUtils.toJsonString(systemMetadata)));
    }
    final Map<TimeseriesFieldSpec, List<Object>> timeseriesFieldValueMap =
        FieldExtractor.extractFields(timeseriesAspect, aspectSpec.getTimeseriesFieldSpecs());
    timeseriesFieldValueMap.forEach((k, v) -> setTimeseriesField(document, k, v));
    finalDocuments.put(getDocId(document, null), document);

    // Create new rows for the member collection fields.
    final Map<TimeseriesFieldCollectionSpec, List<Object>> timeseriesFieldCollectionValueMap =
        FieldExtractor.extractFields(timeseriesAspect, aspectSpec.getTimeseriesFieldCollectionSpecs());
    timeseriesFieldCollectionValueMap.forEach(
        (key, values) -> finalDocuments.putAll(getTimeseriesFieldCollectionDocuments(key, values, commonDocument)));
    return finalDocuments;
  }

  private static ObjectNode getCommonDocument(@Nonnull final Urn urn, final RecordTemplate timeseriesAspect,
      @Nullable final SystemMetadata systemMetadata) {
    if (!timeseriesAspect.data().containsKey(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)) {
      throw new IllegalArgumentException("Input timeseries aspect does not contain a timestampMillis field");
    }
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put(MappingsBuilder.URN_FIELD, urn.toString());
    document.put(MappingsBuilder.TIMESTAMP_FIELD,
        (Long) timeseriesAspect.data().get(MappingsBuilder.TIMESTAMP_MILLIS_FIELD));
    document.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD,
        (Long) timeseriesAspect.data().get(MappingsBuilder.TIMESTAMP_MILLIS_FIELD));
    Object eventGranularity = timeseriesAspect.data().get(MappingsBuilder.EVENT_GRANULARITY);
    if (eventGranularity != null) {
      try {
        document.put(MappingsBuilder.EVENT_GRANULARITY, OBJECT_MAPPER.writeValueAsString(eventGranularity));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to convert eventGranulairty to Json string!", e);
      }
    }
    // PartitionSpec handling
    DataMap partitionSpec = (DataMap) timeseriesAspect.data().get(MappingsBuilder.PARTITION_SPEC);
    if (partitionSpec != null) {
      Object partition = partitionSpec.get(MappingsBuilder.PARTITION_SPEC_PARTITION);
      Object timePartition = partitionSpec.get(MappingsBuilder.PARTITION_SPEC_TIME_PARTITION);
      if (partition != null && timePartition != null) {
        throw new IllegalArgumentException("Both partition and timePartition cannot be specified in partitionSpec!");
      } else if (partition != null) {
        ObjectNode partitionDoc = JsonNodeFactory.instance.objectNode();
        partitionDoc.put(MappingsBuilder.PARTITION_SPEC_PARTITION, partition.toString());
        document.set(MappingsBuilder.PARTITION_SPEC, partitionDoc);
      } else if (timePartition != null) {
        ObjectNode timePartitionDoc = JsonNodeFactory.instance.objectNode();
        try {
          timePartitionDoc.put(MappingsBuilder.PARTITION_SPEC_TIME_PARTITION,
              OBJECT_MAPPER.writeValueAsString(timePartition));
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException("Failed to convert timePartition to Json string!", e);
        }
        document.set(MappingsBuilder.PARTITION_SPEC, timePartitionDoc);
      } else {
        throw new IllegalArgumentException("Both partition and timePartition cannot be null in partitionSpec.");
      }
    }
    String messageId = (String) timeseriesAspect.data().get(MappingsBuilder.MESSAGE_ID_FIELD);
    if (messageId != null) {
      document.put(MappingsBuilder.MESSAGE_ID_FIELD, messageId);
    }

    return document;
  }

  private static void setTimeseriesField(final ObjectNode document, final TimeseriesFieldSpec fieldSpec,
      List<Object> valueList) {
    if (valueList.size() == 0) {
      return;
    }

    JsonNode valueNode;
    switch (fieldSpec.getPegasusSchema().getType()) {
      case INT:
        valueNode = JsonNodeFactory.instance.numberNode((Integer) valueList.get(0));
        break;
      case LONG:
        valueNode = JsonNodeFactory.instance.numberNode((Long) valueList.get(0));
        break;
      case FLOAT:
        valueNode = JsonNodeFactory.instance.numberNode((Float) valueList.get(0));
        break;
      case DOUBLE:
        valueNode = JsonNodeFactory.instance.numberNode((Double) valueList.get(0));
        break;
      case ARRAY:
        ArrayDataSchema dataSchema = (ArrayDataSchema) fieldSpec.getPegasusSchema();
        if (valueList.get(0) instanceof List<?>) {
          // This is the hack for non-stat-collection array fields. They will end up getting oddly serialized to a string otherwise.
          valueList = (List<Object>) valueList.get(0);
        }
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode(valueList.size());
        valueList.stream().map(x -> {
          if (dataSchema.getItems().getType() == DataSchema.Type.RECORD) {
            try {
              return OBJECT_MAPPER.writeValueAsString(x);
            } catch (JsonProcessingException e) {
              throw new IllegalArgumentException("Failed to convert collection element to Json string!", e);
            }
          } else {
            return x.toString();
          }
        }).forEach(arrayNode::add);
        valueNode = JsonNodeFactory.instance.textNode(arrayNode.toString());
        break;
      case RECORD:
        String recordString;
        try {
          recordString = OBJECT_MAPPER.writeValueAsString(valueList.get(0));
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException(
              "Failed to convert record object to Json string!" + valueList.get(0).toString());
        }
        valueNode = JsonNodeFactory.instance.textNode(recordString);
        break;
      default:
        valueNode = JsonNodeFactory.instance.textNode(valueList.get(0).toString());
        break;
    }
    document.set(fieldSpec.getName(), valueNode);
  }

  private static Map<String, JsonNode> getTimeseriesFieldCollectionDocuments(
      final TimeseriesFieldCollectionSpec fieldSpec, final List<Object> values, final ObjectNode commonDocument) {
    return values.stream()
        .map(value -> getTimeseriesFieldCollectionDocument(fieldSpec, value, commonDocument))
        .collect(
            Collectors.toMap(keyDocPair -> getDocId(keyDocPair.getSecond(), keyDocPair.getFirst()), Pair::getSecond));
  }

  private static Pair<String, ObjectNode> getTimeseriesFieldCollectionDocument(
      final TimeseriesFieldCollectionSpec fieldSpec, final Object value, final ObjectNode timeseriesInfoDocument) {
    ObjectNode finalDocument = JsonNodeFactory.instance.objectNode();
    finalDocument.setAll(timeseriesInfoDocument);
    RecordTemplate collectionComponent = (RecordTemplate) value;
    ObjectNode componentDocument = JsonNodeFactory.instance.objectNode();
    Optional<Object> key = RecordUtils.getFieldValue(collectionComponent, fieldSpec.getKeyPath());
    if (!key.isPresent()) {
      throw new IllegalArgumentException(
          String.format("Key %s for timeseries collection field %s is missing", fieldSpec.getKeyPath(),
              fieldSpec.getName()));
    }
    componentDocument.set(fieldSpec.getTimeseriesFieldCollectionAnnotation().getKey(),
        JsonNodeFactory.instance.textNode(key.get().toString()));
    Map<TimeseriesFieldSpec, List<Object>> statFields = FieldExtractor.extractFields(collectionComponent,
        new ArrayList<>(fieldSpec.getTimeseriesFieldSpecMap().values()));
    statFields.forEach((k, v) -> setTimeseriesField(componentDocument, k, v));
    finalDocument.set(fieldSpec.getName(), componentDocument);
    finalDocument.set(MappingsBuilder.IS_EXPLODED_FIELD, JsonNodeFactory.instance.booleanNode(true));
    // Return the pair of component key and the document. We use the key later to build the unique docId.
    return new Pair<>(fieldSpec.getTimeseriesFieldCollectionAnnotation().getCollectionName() + key.get(),
        finalDocument);
  }

  private static String getDocId(@Nonnull JsonNode document, String collectionId) {
    String docId = document.get(MappingsBuilder.TIMESTAMP_MILLIS_FIELD).toString();
    JsonNode eventGranularity = document.get(MappingsBuilder.EVENT_GRANULARITY);
    if (eventGranularity != null) {
      docId += eventGranularity.toString();
    }
    docId += document.get(MappingsBuilder.URN_FIELD).toString();
    if (collectionId != null) {
      docId += collectionId;
    }
    JsonNode messageId = document.get(MappingsBuilder.MESSAGE_ID_FIELD);
    if (messageId != null) {
      docId += messageId.toString();
    }
    JsonNode partitionSpec = document.get(MappingsBuilder.PARTITION_SPEC);
    if (partitionSpec != null) {
      docId += partitionSpec.toString();
    }

    return DigestUtils.md5Hex(docId);
  }
}
