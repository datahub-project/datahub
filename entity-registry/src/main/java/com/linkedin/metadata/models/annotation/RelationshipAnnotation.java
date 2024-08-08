package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;

/** Simple object representation of the @Relationship annotation metadata. */
@Value
@AllArgsConstructor
public class RelationshipAnnotation {

  public static final String ANNOTATION_NAME = "Relationship";
  private static final String NAME_FIELD = "name";
  private static final String ENTITY_TYPES_FIELD = "entityTypes";
  private static final String IS_UPSTREAM_FIELD = "isUpstream";
  private static final String IS_LINEAGE_FIELD = "isLineage";
  private static final String CREATED_ON = "createdOn";
  private static final String CREATED_ACTOR = "createdActor";
  private static final String UPDATED_ON = "updatedOn";
  private static final String UPDATED_ACTOR = "updatedActor";
  private static final String PROPERTIES = "properties";

  private static final String VIA = "via";

  String name;
  List<String> validDestinationTypes;
  boolean isUpstream;
  boolean isLineage;
  String createdOn;
  String createdActor;
  String updatedOn;
  String updatedActor;
  String properties;
  String via;

  @Nonnull
  public static RelationshipAnnotation fromPegasusAnnotationObject(
      @Nonnull final Object annotationObj, @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> name = AnnotationUtils.getField(map, NAME_FIELD, String.class);
    if (!name.isPresent()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation at %s: Invalid field '%s'. Expected type String",
              ANNOTATION_NAME, context, NAME_FIELD));
    }

    final Optional<List> entityTypesList =
        AnnotationUtils.getField(map, ENTITY_TYPES_FIELD, List.class);
    final List<String> entityTypes = new ArrayList<>();
    if (entityTypesList.isPresent()) {
      for (Object entityTypeObj : entityTypesList.get()) {
        if (!String.class.isAssignableFrom(entityTypeObj.getClass())) {
          throw new ModelValidationException(
              String.format(
                  "Failed to validate @%s annotation at %s: Invalid field '%s'. Expected type List<String>",
                  ANNOTATION_NAME, context, ENTITY_TYPES_FIELD));
        }
        entityTypes.add((String) entityTypeObj);
      }
    }

    final Optional<Boolean> isUpstream =
        AnnotationUtils.getField(map, IS_UPSTREAM_FIELD, Boolean.class);
    final Optional<Boolean> isLineage =
        AnnotationUtils.getField(map, IS_LINEAGE_FIELD, Boolean.class);
    final Optional<String> createdOn = AnnotationUtils.getField(map, CREATED_ON, String.class);
    final Optional<String> createdActor =
        AnnotationUtils.getField(map, CREATED_ACTOR, String.class);
    final Optional<String> updatedOn = AnnotationUtils.getField(map, UPDATED_ON, String.class);
    final Optional<String> updatedActor =
        AnnotationUtils.getField(map, UPDATED_ACTOR, String.class);
    final Optional<String> properties = AnnotationUtils.getField(map, PROPERTIES, String.class);
    final Optional<String> via = AnnotationUtils.getField(map, VIA, String.class);

    return new RelationshipAnnotation(
        name.get(),
        entityTypes,
        isUpstream.orElse(true),
        isLineage.orElse(false),
        createdOn.orElse(null),
        createdActor.orElse(null),
        updatedOn.orElse(null),
        updatedActor.orElse(null),
        properties.orElse(null),
        via.orElse(null));
  }

  /**
   * Constructor for backwards compatibility
   *
   * @param name
   * @param entityTypes
   * @param isUpstream
   * @param isLineage
   * @param createdOn
   * @param createdActor
   * @param updatedOn
   * @param updatedActor
   * @param properties
   */
  public RelationshipAnnotation(
      String name,
      List<String> validDestinationTypes,
      boolean isUpstream,
      boolean isLineage,
      String createdOn,
      String createdActor,
      String updatedOn,
      String updatedActor,
      String properties) {
    this(
        name,
        validDestinationTypes,
        isUpstream,
        isLineage,
        createdOn,
        createdActor,
        updatedOn,
        updatedActor,
        properties,
        null);
  }
}
