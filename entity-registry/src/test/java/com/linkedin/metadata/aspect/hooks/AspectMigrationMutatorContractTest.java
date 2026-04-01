package com.linkedin.metadata.aspect.hooks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Reflection-based test.
 *
 * <p>Automatically discovers every concrete {@link AspectMigrationMutator} on the classpath and
 * enforces the version contract rules. No new test class is required when a new mutator is added —
 * the {@link DataProvider} picks it up automatically at runtime.
 */
public class AspectMigrationMutatorContractTest {

  private static final String SCAN_PACKAGE = "com.linkedin.metadata";
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
  private static final String FALLBACK_ASPECT = "ownership";

  private EntityRegistry entityRegistry;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setUp() {
    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockRetriever = mock(AspectRetriever.class);
    when(mockRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(mockRetriever);
  }

  /**
   * Scans {@code com.linkedin.metadata} for every concrete subclass of {@link
   * AspectMigrationMutator} and returns their classes as test data. Each
   */
  @DataProvider(name = "allMutators") // supplies test data to parameterised test methods
  public Object[][] allMutators() {
    Reflections reflections = new Reflections(SCAN_PACKAGE, new SubTypesScanner());
    Set<Class<? extends AspectMigrationMutator>> subclasses =
        reflections.getSubTypesOf(AspectMigrationMutator.class);

    return subclasses.stream()
        .filter(c -> !Modifier.isAbstract(c.getModifiers()))
        .map(c -> new Object[] {c})
        .toArray(Object[][]::new);
  }

  // Version contract
  @Test(dataProvider = "allMutators")
  public void targetVersion_mustBeSourceVersionPlusOne(
      Class<? extends AspectMigrationMutator> clazz) throws Exception {
    AspectMigrationMutator mutator = instantiate(clazz);
    assertEquals(
        mutator.getTargetVersion(),
        mutator.getSourceVersion() + 1,
        clazz.getSimpleName() + ": targetVersion must equal sourceVersion + 1");
  }

  @Test(dataProvider = "allMutators")
  public void sourceVersion_mustBeAtLeastDefaultSchemaVersion(
      Class<? extends AspectMigrationMutator> clazz) throws Exception {

    AspectMigrationMutator mutator = instantiate(clazz);
    assertTrue(
        mutator.getSourceVersion() >= AspectMigrationMutator.DEFAULT_SCHEMA_VERSION,
        clazz.getSimpleName()
            + ": sourceVersion must be >= DEFAULT_SCHEMA_VERSION ("
            + AspectMigrationMutator.DEFAULT_SCHEMA_VERSION
            + ")");
  }

  @Test(dataProvider = "allMutators")
  public void aspectName_mustBeNonBlank(Class<? extends AspectMigrationMutator> clazz)
      throws Exception {
    AspectMigrationMutator mutator = instantiate(clazz);
    assertNotNull(mutator.getAspectName(), clazz.getSimpleName() + ": aspectName must not be null");
    assertFalse(
        mutator.getAspectName().isBlank(),
        clazz.getSimpleName() + ": aspectName must not be blank");
  }

  // Negative test cases to confirm that the mutator is a no-op when preconditions are not met.
  @Test(dataProvider = "allMutators")
  public void writeMutation_itemAlreadyAtTargetVersion_isNoOp(
      Class<? extends AspectMigrationMutator> clazz) throws Exception {
    AspectMigrationMutator mutator = instantiate(clazz);
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(mutator.getTargetVersion()); // already migrated
    ChangeMCP item = buildItem(mutator.getAspectName(), sm);
    if (item == null) {
      // aspect not registered in TestEntityRegistry — skip behavioural check
      return;
    }

    List<Pair<ChangeMCP, Boolean>> results =
        mutator.writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertFalse(
        results.get(0).getSecond(),
        clazz.getSimpleName() + ": must be no-op when already at targetVersion");
    assertEquals(
        (long) item.getSystemMetadata().getSchemaVersion(),
        mutator.getTargetVersion(),
        "schemaVersion must not change");
  }

  @Test(dataProvider = "allMutators")
  public void readMutation_itemAlreadyAtTargetVersion_isNoOp(
      Class<? extends AspectMigrationMutator> clazz) throws Exception {
    AspectMigrationMutator mutator = instantiate(clazz);
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(mutator.getTargetVersion());
    ChangeMCP item = buildItem(mutator.getAspectName(), sm);
    if (item == null) {
      return;
    }

    List<Pair<ReadItem, Boolean>> results =
        mutator.readMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertFalse(
        results.get(0).getSecond(),
        clazz.getSimpleName() + ": read path must be no-op when already at targetVersion");
  }

  @Test(dataProvider = "allMutators")
  public void writeMutation_wrongAspect_isNoOp(Class<? extends AspectMigrationMutator> clazz)
      throws Exception {
    AspectMigrationMutator mutator = instantiate(clazz);

    // Pick an aspect that is definitely different from the mutator's own aspect
    String differentAspect =
        mutator.getAspectName().equals(FALLBACK_ASPECT) ? "schemaMetadata" : FALLBACK_ASPECT;

    ChangeMCP item = buildItem(differentAspect, new SystemMetadata());
    if (item == null) {
      return;
    }

    List<Pair<ChangeMCP, Boolean>> results =
        mutator.writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertFalse(
        results.get(0).getSecond(),
        clazz.getSimpleName() + ": must be no-op for a different aspect");
  }

  @Test(dataProvider = "allMutators")
  public void readMutation_wrongAspect_isNoOp(Class<? extends AspectMigrationMutator> clazz)
      throws Exception {
    AspectMigrationMutator mutator = instantiate(clazz);

    String differentAspect =
        mutator.getAspectName().equals(FALLBACK_ASPECT) ? "schemaMetadata" : FALLBACK_ASPECT;

    ChangeMCP item = buildItem(differentAspect, new SystemMetadata());
    if (item == null) {
      return;
    }

    List<Pair<ReadItem, Boolean>> results =
        mutator.readMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertFalse(
        results.get(0).getSecond(),
        clazz.getSimpleName() + ": read path must be no-op for a different aspect");
  }

  // Helpers

  private AspectMigrationMutator instantiate(Class<? extends AspectMigrationMutator> clazz)
      throws Exception {
    return clazz.getDeclaredConstructor().newInstance();
  }

  /**
   * Builds a minimal {@link ChangeMCP} for the given aspect name. Returns {@code null} when the
   * aspect is not registered in {@link TestEntityRegistry} so callers can skip the assertion.
   */
  @Nullable
  private ChangeMCP buildItem(String aspectName, SystemMetadata sm) {
    if (entityRegistry.getAspectSpecs().get(aspectName) == null) {
      return null;
    }
    RecordTemplate record =
        aspectName.equals(FALLBACK_ASPECT) ? ownershipRecord() : ownershipRecord();
    return TestMCP.builder()
        .urn(DATASET_URN)
        .changeType(ChangeType.UPSERT)
        .entitySpec(entityRegistry.getEntitySpec("dataset"))
        .aspectSpec(entityRegistry.getAspectSpecs().get(aspectName))
        .recordTemplate(record)
        .systemMetadata(sm)
        .build();
  }

  private static Ownership ownershipRecord() {
    Ownership o = new Ownership();
    o.setOwners(new OwnerArray());
    return o;
  }
}
