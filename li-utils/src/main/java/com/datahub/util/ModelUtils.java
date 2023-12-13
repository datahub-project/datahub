package com.datahub.util;

import com.datahub.common.DummySnapshot;
import com.datahub.util.exception.InvalidSchemaException;
import com.datahub.util.validator.AspectValidator;
import com.datahub.util.validator.DeltaValidator;
import com.datahub.util.validator.DocumentValidator;
import com.datahub.util.validator.EntityValidator;
import com.datahub.util.validator.RelationshipValidator;
import com.datahub.util.validator.SnapshotValidator;
import com.datahub.util.validator.ValidationUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.data.template.WrappingArrayTemplate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.reflections.Reflections;

public class ModelUtils {

  private static final ClassLoader CLASS_LOADER = DummySnapshot.class.getClassLoader();
  private static final String METADATA_AUDIT_EVENT_PREFIX = "METADATA_AUDIT_EVENT";

  private ModelUtils() {
    // Util class
  }

  /**
   * Gets the corresponding aspect name for a specific aspect type.
   *
   * @param aspectClass the aspect type
   * @param <T> must be a valid aspect type
   * @return the corresponding aspect name, which is actually the FQCN of type
   */
  public static <T extends DataTemplate> String getAspectName(@Nonnull Class<T> aspectClass) {
    return aspectClass.getCanonicalName();
  }

  /**
   * Gets the corresponding {@link Class} for a given aspect name.
   *
   * @param aspectName the name returned from {@link #getAspectName(Class)}
   * @return the corresponding {@link Class}
   */
  @Nonnull
  public static Class<? extends RecordTemplate> getAspectClass(@Nonnull String aspectName) {
    return getClassFromName(aspectName, RecordTemplate.class);
  }

  /**
   * Returns all supported aspects from an aspect union.
   *
   * @param aspectUnionClass the aspect union type to extract supported aspects from
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @return a set of supported aspects
   */
  @Nonnull
  public static <ASPECT_UNION extends UnionTemplate>
      Set<Class<? extends RecordTemplate>> getValidAspectTypes(
          @Nonnull Class<ASPECT_UNION> aspectUnionClass) {

    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    Set<Class<? extends RecordTemplate>> validTypes = new HashSet<>();
    for (UnionDataSchema.Member member :
        ValidationUtils.getUnionSchema(aspectUnionClass).getMembers()) {
      if (member.getType().getType() == DataSchema.Type.RECORD) {
        String fqcn = ((RecordDataSchema) member.getType()).getBindingName();
        try {
          validTypes.add(CLASS_LOADER.loadClass(fqcn).asSubclass(RecordTemplate.class));
        } catch (ClassNotFoundException | ClassCastException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return validTypes;
  }

  /** Gets a {@link Class} from its FQCN. */
  @Nonnull
  public static <T> Class<? extends T> getClassFromName(
      @Nonnull String className, @Nonnull Class<T> parentClass) {
    try {
      return CLASS_LOADER.loadClass(className).asSubclass(parentClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets a snapshot class given its FQCN.
   *
   * @param className FQCN of snapshot class
   * @return snapshot class that extends {@link RecordTemplate}, associated with className
   */
  @Nonnull
  public static Class<? extends RecordTemplate> getMetadataSnapshotClassFromName(
      @Nonnull String className) {
    Class<? extends RecordTemplate> snapshotClass =
        getClassFromName(className, RecordTemplate.class);
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    return snapshotClass;
  }

  /**
   * Extracts the "urn" field from a snapshot.
   *
   * @param snapshot the snapshot to extract urn from
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate> Urn getUrnFromSnapshot(
      @Nonnull SNAPSHOT snapshot) {
    SnapshotValidator.validateSnapshotSchema(snapshot.getClass());
    return RecordUtils.getRecordTemplateField(
        snapshot, "urn", urnClassForSnapshot(snapshot.getClass()));
  }

  /**
   * Similar to {@link #getUrnFromSnapshot(RecordTemplate)} but extracts from a Snapshot union
   * instead.
   */
  @Nonnull
  public static Urn getUrnFromSnapshotUnion(@Nonnull UnionTemplate snapshotUnion) {
    return getUrnFromSnapshot(RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion));
  }

  /**
   * Extracts the "urn" field from a delta.
   *
   * @param delta the delta to extract urn from
   * @param <DELTA> must be a valid delta model defined in com.linkedin.metadata.delta
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <DELTA extends RecordTemplate> Urn getUrnFromDelta(@Nonnull DELTA delta) {
    DeltaValidator.validateDeltaSchema(delta.getClass());
    return RecordUtils.getRecordTemplateField(delta, "urn", urnClassForDelta(delta.getClass()));
  }

  /**
   * Similar to {@link #getUrnFromDelta(RecordTemplate)} but extracts from a delta union instead.
   */
  @Nonnull
  public static Urn getUrnFromDeltaUnion(@Nonnull UnionTemplate deltaUnion) {
    return getUrnFromDelta(RecordUtils.getSelectedRecordTemplateFromUnion(deltaUnion));
  }

  /**
   * Extracts the "urn" field from a search document.
   *
   * @param document the document to extract urn from
   * @param <DOCUMENT> must be a valid document model defined in com.linkedin.metadata.search
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <DOCUMENT extends RecordTemplate> Urn getUrnFromDocument(
      @Nonnull DOCUMENT document) {
    DocumentValidator.validateDocumentSchema(document.getClass());
    return RecordUtils.getRecordTemplateField(
        document, "urn", urnClassForDocument(document.getClass()));
  }

  /**
   * Extracts the "urn" field from an entity.
   *
   * @param entity the entity to extract urn from
   * @param <ENTITY> must be a valid entity model defined in com.linkedin.metadata.entity
   * @return the extracted {@link Urn}
   */
  @Nonnull
  public static <ENTITY extends RecordTemplate> Urn getUrnFromEntity(@Nonnull ENTITY entity) {
    EntityValidator.validateEntitySchema(entity.getClass());
    return RecordUtils.getRecordTemplateField(
        entity, "urn", urnClassForDocument(entity.getClass()));
  }

  /**
   * Extracts the fields with type urn from a relationship.
   *
   * @param relationship the relationship to extract urn from
   * @param <RELATIONSHIP> must be a valid relationship model defined in
   *     com.linkedin.metadata.relationship
   * @param fieldName name of the field with type urn
   * @return the extracted {@link Urn}
   */
  @Nonnull
  private static <RELATIONSHIP extends RecordTemplate> Urn getUrnFromRelationship(
      @Nonnull RELATIONSHIP relationship, @Nonnull String fieldName) {
    RelationshipValidator.validateRelationshipSchema(relationship.getClass());
    return RecordUtils.getRecordTemplateField(
        relationship, fieldName, urnClassForRelationship(relationship.getClass(), fieldName));
  }

  /** Similar to {@link #getUrnFromRelationship} but extracts from a delta union instead. */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> Urn getSourceUrnFromRelationship(
      @Nonnull RELATIONSHIP relationship) {
    return getUrnFromRelationship(relationship, "source");
  }

  /** Similar to {@link #getUrnFromRelationship} but extracts from a delta union instead. */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> Urn getDestinationUrnFromRelationship(
      @Nonnull RELATIONSHIP relationship) {
    return getUrnFromRelationship(relationship, "destination");
  }

  /**
   * Extracts the list of aspects in a snapshot.
   *
   * @param snapshot the snapshot to extract aspects from
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @return the extracted list of aspects
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate> List<RecordTemplate> getAspectsFromSnapshot(
      @Nonnull SNAPSHOT snapshot) {

    SnapshotValidator.validateSnapshotSchema(snapshot.getClass());
    return getAspects(snapshot);
  }

  /**
   * Extracts given aspect from a snapshot.
   *
   * @param snapshot the snapshot to extract the aspect from
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @param aspectClass the aspect class type to extract from snapshot
   * @return the extracted aspect
   */
  @Nonnull
  public static <SNAPSHOT extends RecordTemplate, ASPECT extends DataTemplate>
      Optional<ASPECT> getAspectFromSnapshot(
          @Nonnull SNAPSHOT snapshot, @Nonnull Class<ASPECT> aspectClass) {

    return getAspectsFromSnapshot(snapshot).stream()
        .filter(aspect -> aspect.getClass().equals(aspectClass))
        .findFirst()
        .map(aspectClass::cast);
  }

  /**
   * Similar to {@link #getAspectsFromSnapshot(RecordTemplate)} but extracts from a snapshot union
   * instead.
   */
  @Nonnull
  public static List<RecordTemplate> getAspectsFromSnapshotUnion(
      @Nonnull UnionTemplate snapshotUnion) {
    return getAspects(RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion));
  }

  @Nonnull
  private static List<RecordTemplate> getAspects(@Nonnull RecordTemplate snapshot) {
    final Class<? extends WrappingArrayTemplate> clazz = getAspectsArrayClass(snapshot.getClass());

    WrappingArrayTemplate aspectArray =
        RecordUtils.getRecordTemplateWrappedField(snapshot, "aspects", clazz);

    final List<RecordTemplate> aspects = new ArrayList<>();
    aspectArray.forEach(
        item -> aspects.add(RecordUtils.getSelectedRecordTemplateFromUnion((UnionTemplate) item)));
    return aspects;
  }

  /**
   * Creates a snapshot with its urn field set.
   *
   * @param snapshotClass the type of snapshot to create
   * @param urn value for the urn field
   * @param aspects value for the aspects field
   * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param <URN> must be a valid URN type
   * @return the created snapshot
   */
  @Nonnull
  public static <
          SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn>
      SNAPSHOT newSnapshot(
          @Nonnull Class<SNAPSHOT> snapshotClass,
          @Nonnull URN urn,
          @Nonnull List<ASPECT_UNION> aspects) {

    SnapshotValidator.validateSnapshotSchema(snapshotClass);

    final Class<? extends WrappingArrayTemplate> aspectArrayClass =
        getAspectsArrayClass(snapshotClass);

    try {
      final SNAPSHOT snapshot = snapshotClass.newInstance();
      RecordUtils.setRecordTemplatePrimitiveField(snapshot, "urn", urn.toString());
      WrappingArrayTemplate aspectArray = aspectArrayClass.newInstance();
      aspectArray.addAll(aspects);
      RecordUtils.setRecordTemplateComplexField(snapshot, "aspects", aspectArray);
      return snapshot;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private static <SNAPSHOT extends RecordTemplate>
      Class<? extends WrappingArrayTemplate> getAspectsArrayClass(
          @Nonnull Class<SNAPSHOT> snapshotClass) {

    try {
      return snapshotClass
          .getMethod("getAspects")
          .getReturnType()
          .asSubclass(WrappingArrayTemplate.class);
    } catch (NoSuchMethodException | ClassCastException e) {
      throw new RuntimeException((e));
    }
  }

  /**
   * Creates an aspect union with a specific aspect set.
   *
   * @param aspectUnionClass the type of aspect union to create
   * @param aspect the aspect to set
   * @param <ASPECT_UNION> must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param <ASPECT> must be a supported aspect type in ASPECT_UNION
   * @return the created aspect union
   */
  @Nonnull
  public static <ASPECT_UNION extends UnionTemplate, ASPECT extends RecordTemplate>
      ASPECT_UNION newAspectUnion(
          @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull ASPECT aspect) {

    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    try {
      ASPECT_UNION aspectUnion = aspectUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(aspectUnion, aspect);
      return aspectUnion;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /** Gets the expected aspect class for a specific kind of snapshot. */
  @Nonnull
  public static Class<? extends UnionTemplate> aspectClassForSnapshot(
      @Nonnull Class<? extends RecordTemplate> snapshotClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);

    String aspectClassName =
        ((TyperefDataSchema)
                ((ArrayDataSchema)
                        ValidationUtils.getRecordSchema(snapshotClass)
                            .getField("aspects")
                            .getType())
                    .getItems())
            .getBindingName();

    return getClassFromName(aspectClassName, UnionTemplate.class);
  }

  /** Gets the expected {@link Urn} class for a specific kind of entity. */
  @Nonnull
  public static Class<? extends Urn> urnClassForEntity(
      @Nonnull Class<? extends RecordTemplate> entityClass) {
    EntityValidator.validateEntitySchema(entityClass);
    return urnClassForField(entityClass, "urn");
  }

  /** Gets the expected {@link Urn} class for a specific kind of snapshot. */
  @Nonnull
  public static Class<? extends Urn> urnClassForSnapshot(
      @Nonnull Class<? extends RecordTemplate> snapshotClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    return urnClassForField(snapshotClass, "urn");
  }

  /** Gets the expected {@link Urn} class for a specific kind of delta. */
  @Nonnull
  public static Class<? extends Urn> urnClassForDelta(
      @Nonnull Class<? extends RecordTemplate> deltaClass) {
    DeltaValidator.validateDeltaSchema(deltaClass);
    return urnClassForField(deltaClass, "urn");
  }

  /** Gets the expected {@link Urn} class for a specific kind of search document. */
  @Nonnull
  public static Class<? extends Urn> urnClassForDocument(
      @Nonnull Class<? extends RecordTemplate> documentClass) {
    DocumentValidator.validateDocumentSchema(documentClass);
    return urnClassForField(documentClass, "urn");
  }

  /** Gets the expected {@link Urn} class for a specific kind of relationship. */
  @Nonnull
  private static Class<? extends Urn> urnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass, @Nonnull String fieldName) {
    RelationshipValidator.validateRelationshipSchema(relationshipClass);
    return urnClassForField(relationshipClass, fieldName);
  }

  /**
   * Gets the expected {@link Urn} class for the source field of a specific kind of relationship.
   */
  @Nonnull
  public static Class<? extends Urn> sourceUrnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass) {
    return urnClassForRelationship(relationshipClass, "source");
  }

  /**
   * Gets the expected {@link Urn} class for the destination field of a specific kind of
   * relationship.
   */
  @Nonnull
  public static Class<? extends Urn> destinationUrnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass) {
    return urnClassForRelationship(relationshipClass, "destination");
  }

  @Nonnull
  private static Class<? extends Urn> urnClassForField(
      @Nonnull Class<? extends RecordTemplate> recordClass, @Nonnull String fieldName) {
    String urnClassName =
        ((DataMap)
                ValidationUtils.getRecordSchema(recordClass)
                    .getField(fieldName)
                    .getType()
                    .getProperties()
                    .get("java"))
            .getString("class");

    return getClassFromName(urnClassName, Urn.class);
  }

  /** Validates a specific snapshot-aspect combination. */
  public static <SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate>
      void validateSnapshotAspect(
          @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    // Make sure that SNAPSHOT's "aspects" array field contains ASPECT_UNION type.
    if (!aspectClassForSnapshot(snapshotClass).equals(aspectUnionClass)) {
      throw new InvalidSchemaException(
          aspectUnionClass.getCanonicalName()
              + " is not a supported aspect class of "
              + snapshotClass.getCanonicalName());
    }
  }

  /** Validates a specific snapshot-URN combination. */
  public static <SNAPSHOT extends RecordTemplate, URN extends Urn> void validateSnapshotUrn(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<URN> urnClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);

    // Make sure that SNAPSHOT's "urn" field uses the correct class or subclasses
    if (!urnClassForSnapshot(snapshotClass).isAssignableFrom(urnClass)) {
      throw new InvalidSchemaException(
          urnClass.getCanonicalName()
              + " is not a supported URN class of "
              + snapshotClass.getCanonicalName());
    }
  }

  /**
   * Creates a relationship union with a specific relationship set.
   *
   * @param relationshipUnionClass the type of relationship union to create
   * @param relationship the relationship to set
   * @param <RELATIONSHIP_UNION> must be a valid relationship union defined in
   *     com.linkedin.metadata.relationship
   * @param <RELATIONSHIP> must be a supported relationship type in ASPECT_UNION
   * @return the created relationship union
   */
  @Nonnull
  public static <RELATIONSHIP_UNION extends UnionTemplate, RELATIONSHIP extends RecordTemplate>
      RELATIONSHIP_UNION newRelationshipUnion(
          @Nonnull Class<RELATIONSHIP_UNION> relationshipUnionClass,
          @Nonnull RELATIONSHIP relationship) {

    RelationshipValidator.validateRelationshipUnionSchema(relationshipUnionClass);

    try {
      RELATIONSHIP_UNION relationshipUnion = relationshipUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(relationshipUnion, relationship);
      return relationshipUnion;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns all entity classes. */
  @Nonnull
  public static Set<Class<? extends RecordTemplate>> getAllEntities() {
    return new Reflections("com.linkedin.metadata.entity")
        .getSubTypesOf(RecordTemplate.class).stream()
            .filter(EntityValidator::isValidEntitySchema)
            .collect(Collectors.toSet());
  }

  /** Get entity type from urn class. */
  @Nonnull
  public static String getEntityTypeFromUrnClass(@Nonnull Class<? extends Urn> urnClass) {
    try {
      return urnClass.getDeclaredField("ENTITY_TYPE").get(null).toString();
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /** Get aspect specific kafka topic name from urn and aspect classes. */
  @Nonnull
  public static <URN extends Urn, ASPECT extends RecordTemplate>
      String getAspectSpecificMAETopicName(@Nonnull URN urn, @Nonnull ASPECT newValue) {
    return String.format(
        "%s_%s_%s",
        METADATA_AUDIT_EVENT_PREFIX,
        urn.getEntityType().toUpperCase(),
        newValue.getClass().getSimpleName().toUpperCase());
  }

  /**
   * Creates an entity union with a specific entity set.
   *
   * @param entityUnionClass the type of entity union to create
   * @param entity the entity to set
   * @param <ENTITY_UNION> must be a valid enity union defined in com.linkedin.metadata.entity
   * @param <ENTITY> must be a supported entity in entity union
   * @return the created entity union
   */
  @Nonnull
  public static <ENTITY_UNION extends UnionTemplate, ENTITY extends RecordTemplate>
      ENTITY_UNION newEntityUnion(
          @Nonnull Class<ENTITY_UNION> entityUnionClass, @Nonnull ENTITY entity) {

    EntityValidator.validateEntityUnionSchema(entityUnionClass);

    try {
      ENTITY_UNION entityUnion = entityUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(entityUnion, entity);
      return entityUnion;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
