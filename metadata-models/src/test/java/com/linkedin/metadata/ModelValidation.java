package com.linkedin.metadata;

import com.google.common.reflect.ClassPath;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.validator.AspectValidator;
import com.linkedin.metadata.validator.DeltaValidator;
import com.linkedin.metadata.validator.EntityValidator;
import com.linkedin.metadata.validator.RelationshipValidator;
import com.linkedin.metadata.validator.SnapshotValidator;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

import static com.linkedin.metadata.ModelValidationConstants.*;
import static org.testng.AssertJUnit.*;


public class ModelValidation {

  @Test
  public void validateEntities() throws Exception {
    List<? extends Class<? extends RecordTemplate>> entities =
        getRecordTemplatesInPackage("com.linkedin.metadata.entity", IGNORED_ENTITY_CLASSES);

    assertFalse("Failed to find any entities", entities.isEmpty());
    entities.forEach(EntityValidator::validateEntitySchema);
  }

  @Test
  public void validateRelationships() throws Exception {
    List<? extends Class<? extends RecordTemplate>> relationships =
        getRecordTemplatesInPackage("com.linkedin.metadata.relationship", IGNORED_RELATIONSHIP_CLASSES);

    assertFalse("Failed to find any relationships", relationships.isEmpty());
    relationships.forEach(RelationshipValidator::validateRelationshipSchema);
  }

  @Test
  public void validateAspects() throws Exception {
    List<? extends Class<? extends UnionTemplate>> aspects =
        getUnionTemplatesInPackage("com.linkedin.metadata.aspect", IGNORED_ASPECT_CLASSES);

    assertFalse("Failed to find any aspects", aspects.isEmpty());
    aspects.forEach(AspectValidator::validateAspectUnionSchema);
  }

  @Test
  public void validateSnapshots() throws Exception {
    List<? extends Class<? extends RecordTemplate>> snapshots =
        getRecordTemplatesInPackage("com.linkedin.metadata.snapshot", IGNORED_SNAPSHOT_CLASSES);


    assertFalse("Failed to find any snapshots", snapshots.isEmpty());
    snapshots.forEach(SnapshotValidator::validateSnapshotSchema);
  }

  @Test
  public void validateDeltas() throws Exception {
    getRecordTemplatesInPackage("com.linkedin.metadata.delta", IGNORED_DELTA_CLASSES).forEach(
        DeltaValidator::validateDeltaSchema);
  }

  private List<? extends Class<? extends UnionTemplate>> getUnionTemplatesInPackage(@Nonnull String packageName,
      @Nonnull Set<Class<? extends UnionTemplate>> ignoreClasses) throws IOException {
    return getClassesInPackage(packageName, UnionTemplate.class, ignoreClasses);
  }

  private List<? extends Class<? extends RecordTemplate>> getRecordTemplatesInPackage(@Nonnull String packageName,
      @Nonnull Set<Class<? extends RecordTemplate>> ignoreClasses) throws IOException {
    return getClassesInPackage(packageName, RecordTemplate.class, ignoreClasses);
  }

  @SuppressWarnings("unchecked")
  private <T> List<? extends Class<? extends T>> getClassesInPackage(@Nonnull String packageName,
      @Nonnull Class<T> parentClass, @Nonnull Set<Class<? extends T>> ignoreClasses) throws IOException {
    return ClassPath.from(ClassLoader.getSystemClassLoader())
        .getTopLevelClasses(packageName)
        .stream()
        .map(classInfo -> classInfo.load())
        .filter(clazz -> parentClass.isAssignableFrom(clazz))
        .map(x -> (Class<? extends T>) x)
        .filter(clazz -> !ignoreClasses.contains(clazz))
        .collect(Collectors.toList());
  }
}
