package com.linkedin.metadata.timeseries.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.extractor.FieldExtractor;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.TemporalStatCollectionFieldSpec;
import com.linkedin.metadata.models.TemporalStatFieldSpec;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * Class that provides a utility function that transforms the timeseries aspect into a document
 */
@Slf4j
public class TimeseriesAspectTransformer {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TimeseriesAspectTransformer() {
  }

  public static List<JsonNode> transform(@Nonnull final Urn urn, @Nonnull final RecordTemplate timeseriesAspect,
      @Nonnull final AspectSpec aspectSpec, @Nullable final SystemMetadata systemMetadata)
      throws JsonProcessingException {
    ObjectNode commonDocument = getCommonDocument(urn, timeseriesAspect, systemMetadata);
    List<JsonNode> finalDocuments = new ArrayList<>();

    // NOTE: We keep the `event` and `systemMetadata` only with the aspect-level record.
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.setAll(commonDocument);
    document.set(MappingsBuilder.IS_EXPLODED_FIELD, JsonNodeFactory.instance.booleanNode(false));
    document.set(MappingsBuilder.EVENT_FIELD, OBJECT_MAPPER.readTree(RecordUtils.toJsonString(timeseriesAspect)));
    if (systemMetadata != null) {
      document.set(MappingsBuilder.SYSTEM_METADATA_FIELD,
          OBJECT_MAPPER.readTree(RecordUtils.toJsonString(systemMetadata)));
    }
    final Map<TemporalStatFieldSpec, List<Object>> temporalStatFields =
        FieldExtractor.extractFields(timeseriesAspect, aspectSpec.getTemporalStatFieldSpecs());
    temporalStatFields.forEach((k, v) -> setTemporalStatField(document, k, v));
    finalDocuments.add(document);

    // Create new rows for the member collection fields.
    final Map<TemporalStatCollectionFieldSpec, List<Object>> temporalStatCollectionFields =
        FieldExtractor.extractFieldArrays(timeseriesAspect, aspectSpec.getTemporalStatCollectionFieldSpecs());
    temporalStatCollectionFields.forEach(
        (key, values) -> finalDocuments.addAll(getTemporalStatCollectionDocuments(key, values, commonDocument)));
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
    return document;
  }

  private static void setTemporalStatField(final ObjectNode document, final TemporalStatFieldSpec fieldSpec,
      final List<Object> valueList) {
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
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode(valueList.size());
        valueList.stream().map(Object::toString).forEach(arrayNode::add);
        valueNode = JsonNodeFactory.instance.textNode(arrayNode.toString());
        break;
      default:
        valueNode = JsonNodeFactory.instance.textNode(valueList.get(0).toString());
        break;
    }
    document.set(fieldSpec.getName(), valueNode);
  }

  private static List<JsonNode> getTemporalStatCollectionDocuments(final TemporalStatCollectionFieldSpec fieldSpec,
      final List<Object> values, final ObjectNode commonDocument) {
    return values.stream()
        .map(value -> getTemporalStatCollectionDocument(fieldSpec, value, commonDocument))
        .collect(Collectors.toList());
  }

  private static ObjectNode getTemporalStatCollectionDocument(final TemporalStatCollectionFieldSpec fieldSpec,
      final Object value, final ObjectNode temporalInfoDocument) {
    ObjectNode finalDocument = JsonNodeFactory.instance.objectNode();
    finalDocument.setAll(temporalInfoDocument);
    RecordTemplate collectionComponent = getRecord((RecordDataSchema) fieldSpec.getPegasusSchema(), (DataMap) value);
    ObjectNode componentDocument = JsonNodeFactory.instance.objectNode();
    Optional<Object> key = FieldExtractor.extractField(collectionComponent, fieldSpec.getKeyPath());
    if (!key.isPresent()) {
      throw new IllegalArgumentException(
          String.format("Key %s for temporal stat collection %s is missing", fieldSpec.getKeyPath(),
              fieldSpec.getName()));
    }
    componentDocument.set("key", JsonNodeFactory.instance.textNode(key.get().toString()));
    Map<TemporalStatFieldSpec, List<Object>> statFields =
        FieldExtractor.extractFields(collectionComponent, fieldSpec.getTemporalStats());
    statFields.forEach((k, v) -> setTemporalStatField(componentDocument, k, v));
    finalDocument.set(fieldSpec.getName(), componentDocument);
    finalDocument.set(MappingsBuilder.IS_EXPLODED_FIELD, JsonNodeFactory.instance.booleanNode(true));
    return finalDocument;
  }

  private static RecordTemplate getRecord(RecordDataSchema dataSchema, DataMap objectDataMap) {
    try {
      return (RecordTemplate) DataTemplateUtil.templateConstructor(
          PegasusUtils.getDataTemplateClassFromSchema(dataSchema, RecordTemplate.class)).newInstance(objectDataMap);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error while extracting collection object: %s", e));
    }
  }
}
