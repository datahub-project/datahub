package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.reflections.Reflections;


/**
 * Factory class to get a map of all entity schemas and aspect schemas under com.linkedin package
 * This lets us fetch the PDL data schema of an arbitrary entity or aspect based on their names
 */
public class DataSchemaFactory {
  private final Map<String, DataSchema> entitySchemas;
  private final Map<String, DataSchema> aspectSchemas;

  private static final String NAME_FIELD = "name";
  private static final DataSchemaFactory INSTANCE = new DataSchemaFactory();

  public DataSchemaFactory() {
    this("com.linkedin");
  }

  public DataSchemaFactory(String classPath) {
    entitySchemas = new HashMap<>();
    aspectSchemas = new HashMap<>();

    Reflections reflections = new Reflections(classPath);
    Set<Class<? extends RecordTemplate>> classes = reflections.getSubTypesOf(RecordTemplate.class);

    classes.stream().map(recordClass -> {
      try {
        return DataTemplateUtil.getSchema(recordClass);
      } catch (Exception e) {
        return null;
      }
    }).filter(Objects::nonNull).forEach(schema -> {
      getName(schema, EntityAnnotation.ANNOTATION_NAME).ifPresent(entityName -> entitySchemas.put(entityName, schema));
      getName(schema, AspectAnnotation.ANNOTATION_NAME).ifPresent(aspectName -> aspectSchemas.put(aspectName, schema));
    });
  }

  private Optional<String> getName(DataSchema dataSchema, String annotationName) {
    return Optional.ofNullable(dataSchema.getProperties().get(annotationName))
        .filter(obj -> Map.class.isAssignableFrom(obj.getClass()))
        .flatMap(obj -> Optional.ofNullable(((Map) obj).get(NAME_FIELD)).map(Object::toString));
  }

  public Optional<DataSchema> getEntitySchema(String entityName) {
    return Optional.ofNullable(entitySchemas.get(entityName));
  }

  public Optional<DataSchema> getAspectSchema(String aspectName) {
    return Optional.ofNullable(aspectSchemas.get(aspectName));
  }

  public static DataSchemaFactory getInstance() {
    return INSTANCE;
  }
}
