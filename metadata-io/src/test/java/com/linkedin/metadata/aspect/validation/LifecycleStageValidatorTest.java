package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LifecycleStageValidatorTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,mydata,PROD)");
  private static final Urn STAGE_URN = UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT");
  private static final Urn NONEXISTENT_URN =
      UrnUtils.getUrn("urn:li:lifecycleStageType:NONEXISTENT");

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(LifecycleStageValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "UPDATE"))
          .supportedEntityAspectNames(
              List.of(new AspectPluginConfig.EntityAspectName("*", STATUS_ASPECT_NAME)))
          .build();

  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;

  private EntityRegistry entityRegistry;
  private LifecycleStageValidator validator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new LifecycleStageValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  @Test
  public void testValidStagePassesValidation() {
    LifecycleStageTypeInfo info = makeStageInfo(null);
    doReturn(new Aspect(info.data()))
        .when(mockAspectRetriever)
        .getLatestAspectObject(eq(STAGE_URN), eq("lifecycleStageTypeInfo"));

    Status status = new Status();
    status.setLifecycleStage(STAGE_URN);

    long errors =
        validator
            .validateProposed(Set.of(buildMCP(DATASET_URN, status)), mockRetrieverContext, null)
            .count();

    assertEquals(errors, 0, "Valid stage should pass validation");
  }

  @Test
  public void testNonexistentStageFailsValidation() {
    when(mockAspectRetriever.getLatestAspectObject(
            eq(NONEXISTENT_URN), eq("lifecycleStageTypeInfo")))
        .thenReturn(null);

    Status status = new Status();
    status.setLifecycleStage(NONEXISTENT_URN);

    long errors =
        validator
            .validateProposed(Set.of(buildMCP(DATASET_URN, status)), mockRetrieverContext, null)
            .count();

    assertEquals(errors, 1, "Non-existent stage should fail validation");
  }

  @Test
  public void testNoLifecycleStagePassesValidation() {
    Status status = new Status();
    status.setRemoved(false);

    long errors =
        validator
            .validateProposed(Set.of(buildMCP(DATASET_URN, status)), mockRetrieverContext, null)
            .count();

    assertEquals(errors, 0, "Status without lifecycleStage should pass validation");
    verify(mockAspectRetriever, never()).getLatestAspectObject(any(Urn.class), anyString());
  }

  @Test
  public void testEntityTypeMatchPassesValidation() {
    LifecycleStageTypeInfo info = makeStageInfo(List.of("dataset", "chart"));
    doReturn(new Aspect(info.data()))
        .when(mockAspectRetriever)
        .getLatestAspectObject(eq(STAGE_URN), eq("lifecycleStageTypeInfo"));

    Status status = new Status();
    status.setLifecycleStage(STAGE_URN);

    long errors =
        validator
            .validateProposed(Set.of(buildMCP(DATASET_URN, status)), mockRetrieverContext, null)
            .count();

    assertEquals(errors, 0, "Stage applicable to dataset should pass for dataset entity");
  }

  @Test
  public void testEntityTypeMismatchFailsValidation() {
    LifecycleStageTypeInfo info = makeStageInfo(List.of("glossaryTerm"));
    doReturn(new Aspect(info.data()))
        .when(mockAspectRetriever)
        .getLatestAspectObject(eq(STAGE_URN), eq("lifecycleStageTypeInfo"));

    Status status = new Status();
    status.setLifecycleStage(STAGE_URN);

    long errors =
        validator
            .validateProposed(Set.of(buildMCP(DATASET_URN, status)), mockRetrieverContext, null)
            .count();

    assertEquals(errors, 1, "Stage scoped to glossaryTerm should fail for dataset entity");
  }

  @Test
  public void testNullEntityTypesAppliesToAll() {
    LifecycleStageTypeInfo info = makeStageInfo(null);
    doReturn(new Aspect(info.data()))
        .when(mockAspectRetriever)
        .getLatestAspectObject(eq(STAGE_URN), eq("lifecycleStageTypeInfo"));

    Status status = new Status();
    status.setLifecycleStage(STAGE_URN);

    long errors =
        validator
            .validateProposed(Set.of(buildMCP(DATASET_URN, status)), mockRetrieverContext, null)
            .count();

    assertEquals(errors, 0, "Stage with null entityTypes should pass for any entity type");
  }

  @Test
  public void testEmptyEntityTypesAppliesToAll() {
    LifecycleStageTypeInfo info = makeStageInfo(List.of());
    doReturn(new Aspect(info.data()))
        .when(mockAspectRetriever)
        .getLatestAspectObject(eq(STAGE_URN), eq("lifecycleStageTypeInfo"));

    Status status = new Status();
    status.setLifecycleStage(STAGE_URN);

    long errors =
        validator
            .validateProposed(Set.of(buildMCP(DATASET_URN, status)), mockRetrieverContext, null)
            .count();

    // Empty entityTypes list means "applies to no types" per the PDL doc,
    // but the validator treats it as "applies to all" since empty != explicit restriction.
    // The real enforcement of "empty = disabled" is at the hideInSearch/search layer.
    assertEquals(errors, 0, "Stage with empty entityTypes should pass (empty != restricted)");
  }

  private TestMCP buildMCP(Urn entityUrn, Status status) {
    return TestMCP.builder()
        .changeType(ChangeType.UPSERT)
        .urn(entityUrn)
        .entitySpec(entityRegistry.getEntitySpec(entityUrn.getEntityType()))
        .aspectSpec(
            entityRegistry
                .getEntitySpec(entityUrn.getEntityType())
                .getAspectSpec(STATUS_ASPECT_NAME))
        .recordTemplate(status)
        .build();
  }

  private static LifecycleStageTypeInfo makeStageInfo(List<String> entityTypes) {
    AuditStamp stamp = new AuditStamp();
    stamp.setTime(0L);
    stamp.setActor(UrnUtils.getUrn("urn:li:corpuser:system"));

    LifecycleStageSettings settings = new LifecycleStageSettings();
    settings.setHideInSearch(true);

    LifecycleStageTypeInfo info = new LifecycleStageTypeInfo();
    info.setName("Draft");
    info.setSettings(settings);
    info.setCreated(stamp);
    info.setLastModified(stamp);
    if (entityTypes != null) {
      info.setEntityTypes(new StringArray(entityTypes));
    }
    return info;
  }
}
