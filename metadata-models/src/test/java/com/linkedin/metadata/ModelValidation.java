package com.linkedin.metadata;

import com.google.common.reflect.ClassPath;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.validator.AspectValidator;
import com.linkedin.metadata.validator.DeltaValidator;
import com.linkedin.metadata.validator.DocumentValidator;
import com.linkedin.metadata.validator.EntityValidator;
import com.linkedin.metadata.validator.RelationshipValidator;
import com.linkedin.metadata.validator.SnapshotValidator;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

import static com.linkedin.metadata.ModelValidationConstants.*;

public class ModelValidation {

  @Test
  public void validateEntities() throws Exception {
    getClassesInPackage("com.linkedin.metadata.entity", IGNORED_ENTITY_CLASSES).forEach(
        EntityValidator::validateEntitySchema);
  }

  @Test
  public void validateRelationships() throws Exception {
    getClassesInPackage("com.linkedin.metadata.relationship", IGNORED_RELATIONSHIP_CLASSES).forEach(
        RelationshipValidator::validateRelationshipSchema);
  }

  @Test
  public void validateDocuments() throws Exception {
    getClassesInPackage("com.linkedin.metadata.search", IGNORED_DOCUMENT_CLASSES).forEach(
        DocumentValidator::validateDocumentSchema);
  }

  @Test
  public void validateAspects() throws Exception {
    getClassesInPackage("com.linkedin.metadata.aspect", IGNORED_ASPECT_CLASSES).forEach(
        AspectValidator::validateAspectUnionSchema);
  }

  @Test
  public void validateSnapshots() throws Exception {
    getClassesInPackage("com.linkedin.metadata.snapshot", IGNORED_SNAPSHOT_CLASSES).forEach(
        SnapshotValidator::validateSnapshotSchema);

    SnapshotValidator.validateUniqueUrn(
        getClassesInPackage("com.linkedin.metadata.snapshot", IGNORED_SNAPSHOT_CLASSES).collect(Collectors.toList()));
  }

  @Test
  public void validateDeltas() throws Exception {
    getClassesInPackage("com.linkedin.metadata.delta", IGNORED_DELTA_CLASSES).forEach(DeltaValidator::validateDeltaSchema);
  }

  private Stream<? extends Class> getClassesInPackage(@Nonnull String packageName, @Nonnull Set<Class> ignoreClasses)
      throws IOException {
    return ClassPath.from(ClassLoader.getSystemClassLoader())
        .getTopLevelClasses(packageName)
        .stream()
        .map(classInfo -> classInfo.load())
        .filter(clazz -> RecordTemplate.class.isAssignableFrom(clazz))
        .filter(clazz -> !ignoreClasses.contains(clazz));
  }
}
