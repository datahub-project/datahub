package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema.Type;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema.Member;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.TemplateOutputCastException;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.data.template.WrappingArrayTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dummy.DummySnapshot;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.validator.AspectValidator;
import com.linkedin.metadata.validator.DeltaValidator;
import com.linkedin.metadata.validator.DocumentValidator;
import com.linkedin.metadata.validator.EntityValidator;
import com.linkedin.metadata.validator.InvalidSchemaException;
import com.linkedin.metadata.validator.RelationshipValidator;
import com.linkedin.metadata.validator.SnapshotValidator;
import com.linkedin.metadata.validator.ValidationUtils;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.reflections.Reflections;
import org.reflections.scanners.Scanner;


public class NewModelUtils {
  private static final ClassLoader CLASS_LOADER = DummySnapshot.class.getClassLoader();
  private static final String METADATA_AUDIT_EVENT_PREFIX = "METADATA_AUDIT_EVENT";

  private NewModelUtils() {
  }

  public static <T extends DataTemplate> String getAspectName(@Nonnull Class<T> aspectClass) {
    return aspectClass.getCanonicalName();
  }

  @Nonnull
  public static Class<? extends RecordTemplate> getAspectClass(@Nonnull String aspectName) {
    return getClassFromName(aspectName, RecordTemplate.class);
  }

  @Nonnull
  public static <ASPECT_UNION extends UnionTemplate> Set<Class<? extends RecordTemplate>> getValidAspectTypes(
      @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    AspectValidator.validateAspectUnionSchema(aspectUnionClass);
    Set<Class<? extends RecordTemplate>> validTypes = new HashSet();
    Iterator var2 = ValidationUtils.getUnionSchema(aspectUnionClass).getMembers().iterator();

    while (var2.hasNext()) {
      Member member = (Member) var2.next();
      if (member.getType().getType() == Type.RECORD) {
        String fqcn = ((RecordDataSchema) member.getType()).getBindingName();

        try {
          validTypes.add(CLASS_LOADER.loadClass(fqcn).asSubclass(RecordTemplate.class));
        } catch (ClassCastException | ClassNotFoundException var6) {
          throw new RuntimeException(var6);
        }
      }
    }

    return validTypes;
  }

  @Nonnull
  public static <T> Class<? extends T> getClassFromName(@Nonnull String className, @Nonnull Class<T> parentClass) {
    try {
      return CLASS_LOADER.loadClass(className).asSubclass(parentClass);
    } catch (ClassNotFoundException var3) {
      throw new RuntimeException(var3);
    }
  }

  @Nonnull
  public static Class<? extends RecordTemplate> getMetadataSnapshotClassFromName(@Nonnull String className) {
    Class<? extends RecordTemplate> snapshotClass = getClassFromName(className, RecordTemplate.class);
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    return snapshotClass;
  }

  @Nonnull
  public static <SNAPSHOT extends RecordTemplate> Urn getUrnFromSnapshot(@Nonnull SNAPSHOT snapshot) {
    SnapshotValidator.validateSnapshotSchema(snapshot.getClass());
    return (Urn) RecordUtils.getRecordTemplateField(snapshot, "urn", urnClassForSnapshot(snapshot.getClass()));
  }

  @Nonnull
  public static Urn getUrnFromSnapshotUnion(@Nonnull UnionTemplate snapshotUnion) {
    return getUrnFromSnapshot(RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion));
  }

  @Nonnull
  public static <DELTA extends RecordTemplate> Urn getUrnFromDelta(@Nonnull DELTA delta) {
    DeltaValidator.validateDeltaSchema(delta.getClass());
    return (Urn) RecordUtils.getRecordTemplateField(delta, "urn", urnClassForDelta(delta.getClass()));
  }

  @Nonnull
  public static Urn getUrnFromDeltaUnion(@Nonnull UnionTemplate deltaUnion) {
    return getUrnFromDelta(RecordUtils.getSelectedRecordTemplateFromUnion(deltaUnion));
  }

  @Nonnull
  public static <DOCUMENT extends RecordTemplate> Urn getUrnFromDocument(@Nonnull DOCUMENT document) {
    DocumentValidator.validateDocumentSchema(document.getClass());
    return (Urn) RecordUtils.getRecordTemplateField(document, "urn", urnClassForDocument(document.getClass()));
  }

  @Nonnull
  public static <ENTITY extends RecordTemplate> Urn getUrnFromEntity(@Nonnull ENTITY entity) {
    EntityValidator.validateEntitySchema(entity.getClass());
    return (Urn) RecordUtils.getRecordTemplateField(entity, "urn", urnClassForDocument(entity.getClass()));
  }

  @Nonnull
  private static <RELATIONSHIP extends RecordTemplate> Urn getUrnFromRelationship(@Nonnull RELATIONSHIP relationship,
      @Nonnull String fieldName) {
    RelationshipValidator.validateRelationshipSchema(relationship.getClass());
    return (Urn) RecordUtils.getRecordTemplateField(relationship, fieldName,
        urnClassForRelationship(relationship.getClass(), fieldName));
  }

  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> Urn getSourceUrnFromRelationship(
      @Nonnull RELATIONSHIP relationship) {
    return getUrnFromRelationship(relationship, "source");
  }

  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> Urn getDestinationUrnFromRelationship(
      @Nonnull RELATIONSHIP relationship) {
    return getUrnFromRelationship(relationship, "destination");
  }

  @Nonnull
  public static <SNAPSHOT extends RecordTemplate> List<Pair<String, RecordTemplate>> getAspectsFromSnapshot(
      @Nonnull SNAPSHOT snapshot) {
    SnapshotValidator.validateSnapshotSchema(snapshot.getClass());
    return getAspects(snapshot);
  }

  @Nonnull
  public static <SNAPSHOT extends RecordTemplate, ASPECT extends DataTemplate> Optional<ASPECT> getAspectFromSnapshot(
      @Nonnull SNAPSHOT snapshot, @Nonnull Class<ASPECT> aspectClass) {
    Optional var10000 = getAspectsFromSnapshot(snapshot).stream().filter((aspect) -> {
      return aspect.getClass().equals(aspectClass);
    }).findFirst();
    Objects.requireNonNull(aspectClass);
    return var10000.map(aspectClass::cast);
  }

  @Nonnull
  public static List<Pair<String, RecordTemplate>> getAspectsFromSnapshotUnion(@Nonnull UnionTemplate snapshotUnion) {
    return getAspects(RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion));
  }

  @Nonnull
  private static List<Pair<String, RecordTemplate>> getAspects(@Nonnull RecordTemplate snapshot) {
    Class<? extends WrappingArrayTemplate> clazz = getAspectsArrayClass(snapshot.getClass());
    WrappingArrayTemplate aspectArray =
        (WrappingArrayTemplate) RecordUtils.getRecordTemplateWrappedField(snapshot, "aspects", clazz);
    List<Pair<String, RecordTemplate>> aspects = new ArrayList();
    aspectArray.forEach((item) -> {
      try {
        RecordTemplate aspect = RecordUtils.getSelectedRecordTemplateFromUnion((UnionTemplate) item);
        String name = PegasusUtils.getAspectNameFromSchema(aspect.schema());
        aspects.add(Pair.of(name, aspect));
      } catch (InvalidSchemaException e) {
        // ignore fields that are not part of the union
      } catch (TemplateOutputCastException e) {
        // ignore fields that are not part of the union
      }
    });
    return aspects;
  }

  @Nonnull
  public static <SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn> SNAPSHOT newSnapshot(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull URN urn, @Nonnull List<ASPECT_UNION> aspects) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    Class aspectArrayClass = getAspectsArrayClass(snapshotClass);

    try {
      SNAPSHOT snapshot = (SNAPSHOT) snapshotClass.newInstance();
      RecordUtils.setRecordTemplatePrimitiveField(snapshot, "urn", urn.toString());
      WrappingArrayTemplate aspectArray = (WrappingArrayTemplate) aspectArrayClass.newInstance();
      aspectArray.addAll(aspects);
      RecordUtils.setRecordTemplateComplexField(snapshot, "aspects", aspectArray);
      return snapshot;
    } catch (IllegalAccessException | InstantiationException var6) {
      throw new RuntimeException(var6);
    }
  }

  @Nonnull
  private static <SNAPSHOT extends RecordTemplate> Class<? extends WrappingArrayTemplate> getAspectsArrayClass(
      @Nonnull Class<SNAPSHOT> snapshotClass) {
    try {
      return snapshotClass.getMethod("getAspects").getReturnType().asSubclass(WrappingArrayTemplate.class);
    } catch (ClassCastException | NoSuchMethodException var2) {
      throw new RuntimeException(var2);
    }
  }

  @Nonnull
  public static <ASPECT_UNION extends UnionTemplate, ASPECT extends RecordTemplate> ASPECT_UNION newAspectUnion(
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull ASPECT aspect) {
    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    try {
      ASPECT_UNION aspectUnion = (ASPECT_UNION) aspectUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(aspectUnion, aspect);
      return aspectUnion;
    } catch (IllegalAccessException | InstantiationException var3) {
      throw new RuntimeException(var3);
    }
  }

  @Nonnull
  public static <ASPECT extends RecordTemplate> AspectVersion newAspectVersion(@Nonnull Class<ASPECT> aspectClass,
      long version) {
    AspectVersion aspectVersion = new AspectVersion();
    aspectVersion.setAspect(getAspectName(aspectClass));
    aspectVersion.setVersion(version);
    return aspectVersion;
  }

  @Nonnull
  public static Class<? extends UnionTemplate> aspectClassForSnapshot(
      @Nonnull Class<? extends RecordTemplate> snapshotClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    String aspectClassName = ((TyperefDataSchema) ((ArrayDataSchema) ValidationUtils.getRecordSchema(snapshotClass)
        .getField("aspects")
        .getType()).getItems()).getBindingName();
    return getClassFromName(aspectClassName, UnionTemplate.class);
  }

  @Nonnull
  public static Class<? extends Urn> urnClassForEntity(@Nonnull Class<? extends RecordTemplate> entityClass) {
    EntityValidator.validateEntitySchema(entityClass);
    return urnClassForField(entityClass, "urn");
  }

  @Nonnull
  public static Class<? extends Urn> urnClassForSnapshot(@Nonnull Class<? extends RecordTemplate> snapshotClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    return urnClassForField(snapshotClass, "urn");
  }

  @Nonnull
  public static Class<? extends Urn> urnClassForDelta(@Nonnull Class<? extends RecordTemplate> deltaClass) {
    DeltaValidator.validateDeltaSchema(deltaClass);
    return urnClassForField(deltaClass, "urn");
  }

  @Nonnull
  public static Class<? extends Urn> urnClassForDocument(@Nonnull Class<? extends RecordTemplate> documentClass) {
    DocumentValidator.validateDocumentSchema(documentClass);
    return urnClassForField(documentClass, "urn");
  }

  @Nonnull
  private static Class<? extends Urn> urnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass, @Nonnull String fieldName) {
    RelationshipValidator.validateRelationshipSchema(relationshipClass);
    return urnClassForField(relationshipClass, fieldName);
  }

  @Nonnull
  public static Class<? extends Urn> sourceUrnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass) {
    return urnClassForRelationship(relationshipClass, "source");
  }

  @Nonnull
  public static Class<? extends Urn> destinationUrnClassForRelationship(
      @Nonnull Class<? extends RecordTemplate> relationshipClass) {
    return urnClassForRelationship(relationshipClass, "destination");
  }

  @Nonnull
  private static Class<? extends Urn> urnClassForField(@Nonnull Class<? extends RecordTemplate> recordClass,
      @Nonnull String fieldName) {
    String urnClassName = ((DataMap) ValidationUtils.getRecordSchema(recordClass)
        .getField(fieldName)
        .getType()
        .getProperties()
        .get("java")).getString("class");
    return getClassFromName(urnClassName, Urn.class);
  }

  public static <SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate> void validateSnapshotAspect(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    AspectValidator.validateAspectUnionSchema(aspectUnionClass);
    if (!aspectClassForSnapshot(snapshotClass).equals(aspectUnionClass)) {
      throw new InvalidSchemaException(aspectUnionClass.getCanonicalName() + " is not a supported aspect class of "
          + snapshotClass.getCanonicalName());
    }
  }

  public static <SNAPSHOT extends RecordTemplate, URN extends Urn> void validateSnapshotUrn(
      @Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<URN> urnClass) {
    SnapshotValidator.validateSnapshotSchema(snapshotClass);
    if (!urnClassForSnapshot(snapshotClass).isAssignableFrom(urnClass)) {
      throw new InvalidSchemaException(
          urnClass.getCanonicalName() + " is not a supported URN class of " + snapshotClass.getCanonicalName());
    }
  }

  @Nonnull
  public static <RELATIONSHIP_UNION extends UnionTemplate, RELATIONSHIP extends RecordTemplate> RELATIONSHIP_UNION newRelationshipUnion(
      @Nonnull Class<RELATIONSHIP_UNION> relationshipUnionClass, @Nonnull RELATIONSHIP relationship) {
    RelationshipValidator.validateRelationshipUnionSchema(relationshipUnionClass);

    try {
      RELATIONSHIP_UNION relationshipUnion = (RELATIONSHIP_UNION) relationshipUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(relationshipUnion, relationship);
      return relationshipUnion;
    } catch (IllegalAccessException | InstantiationException var3) {
      throw new RuntimeException(var3);
    }
  }

  @Nonnull
  public static Set<Class<? extends RecordTemplate>> getAllEntities() {
    return (Set) (new Reflections("com.linkedin.metadata.entity", new Scanner[0])).getSubTypesOf(RecordTemplate.class)
        .stream()
        .filter(EntityValidator::isValidEntitySchema)
        .collect(Collectors.toSet());
  }

  @Nonnull
  public static String getEntityTypeFromUrnClass(@Nonnull Class<? extends Urn> urnClass) {
    try {
      return urnClass.getDeclaredField("ENTITY_TYPE").get((Object) null).toString();
    } catch (IllegalAccessException | NoSuchFieldException var2) {
      throw new RuntimeException(var2);
    }
  }

  @Nonnull
  public static <URN extends Urn, ASPECT extends RecordTemplate> String getAspectSpecificMAETopicName(@Nonnull URN urn,
      @Nonnull ASPECT newValue) {
    return String.format("%s_%s_%s", "METADATA_AUDIT_EVENT", urn.getEntityType().toUpperCase(),
        newValue.getClass().getSimpleName().toUpperCase());
  }

  public static boolean isCommonAspect(@Nonnull Class<? extends RecordTemplate> clazz) {
    return clazz.getPackage().getName().startsWith("com.linkedin.common");
  }

  @Nonnull
  public static <ENTITY_UNION extends UnionTemplate, ENTITY extends RecordTemplate> ENTITY_UNION newEntityUnion(
      @Nonnull Class<ENTITY_UNION> entityUnionClass, @Nonnull ENTITY entity) {
    EntityValidator.validateEntityUnionSchema(entityUnionClass);

    try {
      ENTITY_UNION entityUnion = (ENTITY_UNION) entityUnionClass.newInstance();
      RecordUtils.setSelectedRecordTemplateInUnion(entityUnion, entity);
      return entityUnion;
    } catch (IllegalAccessException | InstantiationException var3) {
      throw new RuntimeException(var3);
    }
  }
}
