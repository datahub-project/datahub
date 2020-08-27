package com.linkedin.metadata.generator;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.pegasus.generator.DataSchemaParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.generator.SchemaGeneratorConstants.*;


/***
 * Parse the property annotations from the pdl schema.
 */
@Slf4j
public class SchemaAnnotationRetriever {

  private final DataSchemaParser _dataSchemaParser;

  public SchemaAnnotationRetriever(@Nonnull String resolverPath) {
    _dataSchemaParser = new DataSchemaParser(resolverPath);
  }

  public List<EventSpec> generate(@Nonnull String[] sources) throws IOException {

    final DataSchemaParser.ParseResult parseResult = _dataSchemaParser.parseSources(sources);
    final List<EventSpec> eventSpecs = new ArrayList<>();
    for (DataSchema dataSchema : parseResult.getSchemaAndLocations().keySet()) {
      if (dataSchema.getType() == DataSchema.Type.RECORD) {
        generate((RecordDataSchema) dataSchema, eventSpecs);
      }
    }
    return eventSpecs;
  }

  private void generate(@Nonnull RecordDataSchema schema, @Nonnull List<EventSpec> specs) {
    final Map<String, Object> props = schema.getProperties();
    EventSpec eventSpec = null;
    Map<String, Object> annotationInfo = null;
    if (props.containsKey(ASPECT)) {
      eventSpec = new EventSpec();
      eventSpec.setSpecType(METADATA_CHANGE_EVENT);
      annotationInfo = (Map<String, Object>) props.get(ASPECT);
    } else {
      log.debug(String.format("No recognized property annotations are presented in %s.", schema.getFullName()));
      return;
    }
    specs.add(eventSpec);
    if (annotationInfo != null && annotationInfo.containsKey(ENTITY_URNS)) {
      eventSpec.setUrnSet(new HashSet<>((List) annotationInfo.get(ENTITY_URNS)));
    }
    if (annotationInfo != null && annotationInfo.containsKey(DELTA)) {
      eventSpec.setDelta((String) annotationInfo.get(DELTA));
    }
    eventSpec.setNamespace(schema.getNamespace());
    eventSpec.setValueType(schema.getFullName());
  }
}