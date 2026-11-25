package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.fabrictype.CustomFabricTypeInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.key.CustomFabricTypeKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FabricTypeValidatorTest {

  private static final EntityRegistry REGISTRY = TestOperationContexts.defaultEntityRegistry();
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,MY_ENV)");
  private static final Urn DATA_FLOW = UrnUtils.getUrn("urn:li:dataFlow:(airflow,flow,PROD)");
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:admin");
  private static final Urn PROD_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.down,PROD)");
  private static final Urn DATA_JOB =
      UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD),job)");

  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private FabricTypeValidator validator;
  private final Map<Urn, List<RecordTemplate>> types = new HashMap<>();

  @BeforeMethod
  public void setUp() {
    enabled.set(true);
    types.clear();
    validator = FabricTypeValidator.create(REGISTRY, enabled::get);
  }

  @Test
  public void scopeIsEveryFabricTypeNameField() {
    List<String> scope =
        FabricTypeNameFields.discover(REGISTRY).stream()
            .map(
                field ->
                    String.join(
                        ".",
                        field.getEntityName(),
                        field.getAspectName(),
                        field.getFieldName(),
                        String.valueOf(field.isKeyAspect()),
                        String.valueOf(field.getSearchFieldName())))
            .collect(Collectors.toList());

    assertEquals(
        scope,
        List.of(
            "container.containerProperties.env.false.env",
            "dataFlow.dataFlowInfo.env.false.env",
            "dataJob.dataJobInfo.env.false.env",
            "dataPlatformInstance.icebergWarehouseInfo.env.false.null",
            "dataProcess.dataProcessKey.origin.true.origin",
            "dataset.datasetKey.origin.true.origin",
            "mlModel.mlModelKey.origin.true.origin",
            "mlModelDeployment.mlModelDeploymentKey.origin.true.origin",
            "mlModelGroup.mlModelGroupKey.origin.true.origin"));
  }

  @Test
  public void restoreIndicesIsOutOfScope() {
    assertTrue(validator.shouldApply(ChangeType.CREATE, DATASET, "datasetKey"));
    assertTrue(validator.shouldApply(ChangeType.UPSERT, DATA_FLOW, "dataFlowInfo"));
    assertTrue(validator.shouldApply(ChangeType.PATCH, DATA_FLOW, "dataFlowInfo"));
    assertFalse(validator.shouldApply(ChangeType.RESTATE, DATASET, "datasetKey"));
    assertFalse(validator.shouldApply(ChangeType.RESTATE, DATA_FLOW, "dataFlowInfo"));
    assertFalse(validator.shouldApply(ChangeType.UPSERT, DATASET, "datasetProperties"));
  }

  @Test
  public void builtInEnvsNeedNoLookupEvenWhenCustomTypesAreOff() {
    enabled.set(false);
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);

    assertTrue(
        validate(retriever, retriever, datasetKey("PROD"), null, dataFlowInfo("DEV"), null)
            .isEmpty());
    verifyNoInteractions(retriever);
  }

  @Test
  public void customEnvIsRejectedWhenCustomTypesAreOff() {
    enabled.set(false);
    register("MY_ENV", null, null);

    assertEquals(validate(datasetKey("MY_ENV"), null).size(), 1);
    assertEquals(validate(dataFlowInfo("MY_ENV"), dataFlowInfo("PROD")).size(), 1);
  }

  @Test
  public void flagIsReadOnEveryBatch() {
    register("MY_ENV", null, null);
    enabled.set(false);
    assertEquals(validate(datasetKey("MY_ENV"), null).size(), 1);
    enabled.set(true);
    assertTrue(validate(datasetKey("MY_ENV"), null).isEmpty());
  }

  @Test
  public void registeredCustomEnvIsAccepted() {
    register("MY_ENV", null, null);

    assertTrue(validate(datasetKey("MY_ENV"), null).isEmpty());
    assertTrue(validate(dataFlowInfo("MY_ENV"), dataFlowInfo("PROD")).isEmpty());
  }

  @Test
  public void unregisteredCustomEnvIsRejectedWithTheUrnToCreate() {
    List<AspectValidationException> errors = validate(datasetKey("MY_ENV"), null);

    assertEquals(errors.size(), 1);
    assertTrue(errors.get(0).getMessage().contains("urn:li:customFabricType:MY_ENV"));
    assertEquals(validate(dataFlowInfo("MY_ENV"), null).size(), 1);
  }

  @Test
  public void malformedEnvIsRejectedWithoutLookup() {
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);

    assertEquals(
        validate(retriever, retriever, dataFlowInfo("my_env"), null, null, null).size(), 1);
    verifyNoInteractions(retriever);
  }

  @Test
  public void softDeletedTypeIsRejectedForNewUses() {
    register("MY_ENV", new Status().setRemoved(true), null);

    assertEquals(validate(datasetKey("MY_ENV"), null).size(), 1);
  }

  @Test
  public void deprecatedTypeIsRejectedForNewUsesWithNoteAndReplacement() {
    Urn replacement = UrnUtils.getUrn("urn:li:customFabricType:NEW_ENV");
    register("MY_ENV", null, deprecated("Moved.").setReplacement(replacement));

    List<AspectValidationException> errors = validate(datasetKey("MY_ENV"), null);
    assertEquals(errors.size(), 1);
    assertTrue(errors.get(0).getMessage().contains("Moved."));
    assertTrue(errors.get(0).getMessage().contains(replacement.toString()));

    assertEquals(validate(dataFlowInfo("MY_ENV"), dataFlowInfo("PROD")).size(), 1);
  }

  @Test
  public void undeprecatedTypeIsAcceptedAgain() {
    register("MY_ENV", null, deprecated("Retired.").setDeprecated(false));

    assertTrue(validate(datasetKey("MY_ENV"), null).isEmpty());
  }

  /** D8: whatever the reason a new use would be refused, keeping the stored value is allowed. */
  @Test
  public void unchangedStoredEnvIsAlwaysKept() {
    // Unregistered.
    assertTrue(validate(dataFlowInfo("MY_ENV"), dataFlowInfo("MY_ENV")).isEmpty());
    // Soft-deleted.
    register("DELETED_ENV", new Status().setRemoved(true), null);
    assertTrue(validate(dataFlowInfo("DELETED_ENV"), dataFlowInfo("DELETED_ENV")).isEmpty());
    // Deprecated.
    register("OLD_ENV", null, deprecated("Retired."));
    assertTrue(validate(dataFlowInfo("OLD_ENV"), dataFlowInfo("OLD_ENV")).isEmpty());
    // Custom types off.
    enabled.set(false);
    assertTrue(validate(dataFlowInfo("MY_ENV"), dataFlowInfo("MY_ENV")).isEmpty());
    // Changing to any of them is a new use.
    assertEquals(validate(dataFlowInfo("OLD_ENV"), dataFlowInfo("MY_ENV")).size(), 1);
  }

  @Test
  public void cachedMissIsRecheckedUncached() {
    MockAspectRetriever cached = spy(new MockAspectRetriever(Map.of()));
    register("MY_ENV", null, null);
    MockAspectRetriever uncached = spy(new MockAspectRetriever(types));

    assertTrue(validate(uncached, cached, datasetKey("MY_ENV"), null, null, null).isEmpty());
    verify(cached, times(1)).getLatestAspectObjects(any(), anySet(), anySet());
    verify(uncached, times(1)).getLatestAspectObjects(any(), anySet(), anySet());
  }

  @Test
  public void cachedHitIsNotRechecked() {
    register("MY_ENV", null, null);
    MockAspectRetriever cached = spy(new MockAspectRetriever(types));
    AspectRetriever uncached = mock(AspectRetriever.class);

    assertTrue(validate(uncached, cached, datasetKey("MY_ENV"), null, null, null).isEmpty());
    verifyNoInteractions(uncached);
  }

  @Test
  public void oneLookupPerBatch() {
    register("MY_ENV", null, null);
    register("OTHER_ENV", null, null);
    MockAspectRetriever retriever = spy(new MockAspectRetriever(types));

    List<ChangeMCP> items = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      items.add(item(datasetKey(i % 2 == 0 ? "MY_ENV" : "OTHER_ENV"), null));
    }
    assertTrue(validateItems(retriever, retriever, items).isEmpty());
    verify(retriever, times(1)).getLatestAspectObjects(any(), anySet(), anySet());
  }

  @Test
  public void customTypeEntityAspectsAreNotInScope() {
    assertNull(validator.getFields().get("customFabricType"));
  }

  @Test
  public void referencesAreFoundFromTheSchema() {
    FabricTypeNameFields.UrnReferences references =
        FabricTypeNameFields.discoverReferences(REGISTRY);

    // MLModelUrn and DataProcessUrn also hold an env, but only snapshots use them, not aspects.
    assertEquals(references.getEnvPartByTyperef(), Map.of("com.linkedin.common.DatasetUrn", 2));
    assertTrue(references.contains("dataset", "upstreamLineage"));
    assertTrue(references.contains("dataJob", "dataJobInputOutput"));
    assertFalse(references.contains("dataset", "datasetProperties"));
    assertTrue(validator.shouldApply(ChangeType.UPSERT, PROD_DATASET, "upstreamLineage"));
    assertFalse(validator.shouldApply(ChangeType.RESTATE, PROD_DATASET, "upstreamLineage"));
  }

  @Test
  public void builtInReferencesNeedNoLookupEvenWhenCustomTypesAreOff() {
    enabled.set(false);
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);

    assertTrue(
        validate(retriever, retriever, upstreams("PROD", "DEV"), null, jobIo("QA", "PROD"), null)
            .isEmpty());
    verifyNoInteractions(retriever);
  }

  /** Parity with master, which rejected any non-built-in env in a typed reference. */
  @Test
  public void customReferenceIsRejectedWhenCustomTypesAreOff() {
    enabled.set(false);
    register("MY_ENV", null, null);

    List<AspectValidationException> errors = validate(upstreams("PROD", "MY_ENV"), null);
    assertEquals(errors.size(), 1);
    assertTrue(errors.get(0).getMessage().contains(datasetUrn("MY_ENV")));
    assertEquals(validate(jobIo("PROD", "MY_ENV"), null).size(), 1);
  }

  @Test
  public void referenceToAnUnregisteredOrDeletedEnvIsRejected() {
    register("GONE_ENV", new Status().setRemoved(true), null);

    List<AspectValidationException> typo = validate(upstreams("PORD"), null);
    assertEquals(typo.size(), 1);
    assertTrue(typo.get(0).getMessage().contains("urn:li:customFabricType:PORD"));
    assertEquals(validate(jobIo("GONE_ENV", "PROD"), null).size(), 1);
  }

  @Test
  public void referenceToARegisteredOrDeprecatedEnvIsAccepted() {
    register("MY_ENV", null, null);
    register("OLD_ENV", null, deprecated("Retired."));

    assertTrue(validate(upstreams("MY_ENV", "OLD_ENV", "PROD"), null).isEmpty());
    assertTrue(validate(jobIo("MY_ENV", "OLD_ENV"), null).isEmpty());
  }

  /** D8 for references: a reference already stored is kept, whatever its env's state. */
  @Test
  public void unchangedReferenceIsKept() {
    enabled.set(false);

    assertTrue(validate(upstreams("PORD", "PROD"), upstreams("PORD")).isEmpty());
    assertEquals(validate(upstreams("PORD", "OTHER_ENV"), upstreams("PORD")).size(), 1);
  }

  @Test
  public void onlyFieldsLeadingToAUrnWithEnvAreScanned() {
    FabricTypeNameFields.UrnReferences references =
        FabricTypeNameFields.discoverReferences(REGISTRY);

    List<String> schemaMetadata =
        references.getReferencingFields().get("com.linkedin.schema.SchemaMetadata");
    assertTrue(schemaMetadata.contains("dataset"));
    assertFalse(schemaMetadata.contains("fields"));
    assertEquals(
        references.getReferencingFields().get("com.linkedin.dataset.UpstreamLineage"),
        List.of("upstreams"));
  }

  @Test
  public void referenceInASchemaIsFoundWithoutVisitingItsFields() {
    DataList fields = new DataList();
    for (int i = 0; i < 1000; i++) {
      DataMap field = new DataMap();
      field.put("fieldPath", "col_" + i);
      fields.add(field);
    }
    DataMap data = new DataMap();
    data.put("dataset", datasetUrn("PORD"));
    data.put("fields", fields);

    Map<String, String> customs =
        FabricTypeNameFields.discoverReferences(REGISTRY)
            .customEnvs(
                data,
                REGISTRY
                    .getEntitySpec("dataset")
                    .getAspectSpec("schemaMetadata")
                    .getPegasusSchema());
    assertEquals(customs, Map.of(datasetUrn("PORD"), "PORD"));
  }

  @Test
  public void proposalRejectsAnUnregisteredEnvWithoutPreCommit() {
    List<AspectValidationException> typo =
        validateProposed(proposed(DATA_FLOW, "dataFlowInfo", dataFlowInfo("STAGING")));
    assertEquals(typo.size(), 1);
    assertTrue(typo.get(0).getMessage().contains("urn:li:customFabricType:STAGING"));
    assertEquals(
        validateProposed(proposed(PROD_DATASET, "upstreamLineage", upstreams("PORD"))).size(), 1);

    register("MY_ENV", null, null);
    assertTrue(
        validateProposed(proposed(DATA_FLOW, "dataFlowInfo", dataFlowInfo("MY_ENV"))).isEmpty());
  }

  @Test
  public void proposalChecksTheEnvInTheEntityUrn() {
    ChangeMCP item = proposed(DATASET, "datasetProperties", new DatasetProperties());

    assertEquals(validateProposed(item).size(), 1);

    register("MY_ENV", null, null);
    assertTrue(validateProposed(item).isEmpty());
  }

  @Test
  public void proposalKeepsAStoredEnvWhateverItsState() {
    enabled.set(true);
    register("MY_ENV", null, null);
    types.put(DATASET, List.of(datasetKey("MY_ENV")));
    types.put(DATA_FLOW, List.of(dataFlowInfo("MY_ENV")));

    assertTrue(
        validateProposed(proposed(DATASET, "datasetProperties", new DatasetProperties()))
            .isEmpty());
    assertTrue(
        validateProposed(proposed(DATA_FLOW, "dataFlowInfo", dataFlowInfo("MY_ENV"))).isEmpty());
    assertEquals(
        validateProposed(proposed(DATA_FLOW, "dataFlowInfo", dataFlowInfo("OTHER_ENV"))).size(), 1);
  }

  /** The stored aspect is only read for what a decision needs; the rest of the item is not. */
  @Test
  @SuppressWarnings("unchecked")
  public void proposalReadsOnlyTheAspectThatHoldsTheValue() {
    register("MY_ENV", null, null);
    MockAspectRetriever retriever = spy(new MockAspectRetriever(types));

    assertTrue(
        validateProposed(
                retriever,
                List.of(
                    proposed(DATASET, "datasetProperties", new DatasetProperties()),
                    proposed(DATASET, "schemaMetadata", new SchemaMetadata()),
                    proposed(DATA_FLOW, "dataFlowInfo", dataFlowInfo("MY_ENV"))))
            .isEmpty());

    ArgumentCaptor<Set<String>> aspectNames = ArgumentCaptor.forClass(Set.class);
    verify(retriever, atLeastOnce()).getLatestAspectObjects(any(), anySet(), aspectNames.capture());
    Set<Set<String>> reads = new HashSet<>(aspectNames.getAllValues());
    reads.removeIf(names -> names.contains("customFabricTypeKey"));
    assertEquals(reads, Set.of(Set.of("datasetKey"), Set.of("dataFlowInfo")));
  }

  /**
   * With custom types off, a new custom use is refused at request time, so an async write is not
   * acknowledged and queued for a consumer to reject later.
   */
  @Test
  public void proposalRefusesNewCustomUseWhenCustomTypesAreOff() {
    enabled.set(false);
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);

    assertEquals(
        validateProposed(
                retriever, List.of(proposed(DATA_FLOW, "dataFlowInfo", dataFlowInfo("PORD"))))
            .size(),
        1);
  }

  @Test
  public void proposalOfBuiltInEnvsDoesNoLookup() {
    enabled.set(false);
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);

    assertTrue(
        validateProposed(
                retriever,
                List.of(
                    proposed(DATA_FLOW, "dataFlowInfo", dataFlowInfo("DEV")),
                    proposed(PROD_DATASET, "upstreamLineage", upstreams("PROD", "DEV")),
                    proposed(PROD_DATASET, "datasetProperties", new DatasetProperties())))
            .isEmpty());
    verifyNoInteractions(retriever);
  }

  @Test
  public void proposalSkipsItemsWithoutARecord() {
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);

    assertTrue(
        validateProposed(retriever, List.of(proposed(DATA_FLOW, "dataFlowInfo", null))).isEmpty());
    verifyNoInteractions(retriever);
  }

  private List<AspectValidationException> validateProposed(ChangeMCP item) {
    return validateProposed(new MockAspectRetriever(types), List.of(item));
  }

  private List<AspectValidationException> validateProposed(
      AspectRetriever retriever, List<ChangeMCP> items) {
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(mock(GraphRetriever.class))
            .aspectRetriever(retriever)
            .cachingAspectRetriever(
                retriever instanceof CachingAspectRetriever
                    ? (CachingAspectRetriever) retriever
                    : null)
            .build();
    return validator
        .validateProposedAspects(OperationFingerprint.EMPTY, items, retrieverContext)
        .collect(Collectors.toList());
  }

  private static ChangeMCP proposed(Urn urn, String aspectName, @Nullable RecordTemplate record) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getEntitySpec()).thenReturn(REGISTRY.getEntitySpec(urn.getEntityType()));
    when(item.getAspectName()).thenReturn(aspectName);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getRecordTemplate()).thenReturn(record);
    return item;
  }

  private void register(String id, @Nullable Status status, @Nullable Deprecation deprecation) {
    List<RecordTemplate> aspects = new ArrayList<>();
    aspects.add(new CustomFabricTypeKey().setId(id));
    aspects.add(new CustomFabricTypeInfo().setDisplayName(id));
    if (status != null) {
      aspects.add(status);
    }
    if (deprecation != null) {
      aspects.add(deprecation);
    }
    types.put(FabricTypeValidator.urn(id), aspects);
  }

  private List<AspectValidationException> validate(
      RecordTemplate proposed, @Nullable RecordTemplate previous) {
    MockAspectRetriever retriever = new MockAspectRetriever(types);
    return validate(retriever, retriever, proposed, previous, null, null);
  }

  private List<AspectValidationException> validate(
      AspectRetriever uncached,
      CachingAspectRetriever cached,
      RecordTemplate proposed,
      @Nullable RecordTemplate previous,
      @Nullable RecordTemplate secondProposed,
      @Nullable RecordTemplate secondPrevious) {
    List<ChangeMCP> items = new ArrayList<>();
    items.add(item(proposed, previous));
    if (secondProposed != null) {
      items.add(item(secondProposed, secondPrevious));
    }
    return validateItems(uncached, cached, items);
  }

  private List<AspectValidationException> validateItems(
      AspectRetriever uncached, CachingAspectRetriever cached, List<ChangeMCP> items) {
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(mock(GraphRetriever.class))
            .aspectRetriever(uncached)
            .cachingAspectRetriever(cached)
            .build();
    return validator
        .validatePreCommitAspects(OperationFingerprint.EMPTY, items, retrieverContext)
        .collect(Collectors.toList());
  }

  private static ChangeMCP item(RecordTemplate proposed, @Nullable RecordTemplate previous) {
    boolean key = proposed instanceof DatasetKey;
    Urn urn;
    String aspectName;
    if (key) {
      urn = DATASET;
      aspectName = "datasetKey";
    } else if (proposed instanceof UpstreamLineage) {
      urn = PROD_DATASET;
      aspectName = "upstreamLineage";
    } else if (proposed instanceof DataJobInputOutput) {
      urn = DATA_JOB;
      aspectName = "dataJobInputOutput";
    } else {
      urn = DATA_FLOW;
      aspectName = "dataFlowInfo";
    }
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getEntitySpec()).thenReturn(REGISTRY.getEntitySpec(urn.getEntityType()));
    when(item.getAspectName()).thenReturn(aspectName);
    when(item.getChangeType()).thenReturn(key ? ChangeType.CREATE : ChangeType.UPSERT);
    when(item.getRecordTemplate()).thenReturn(proposed);
    when(item.getPreviousRecordTemplate()).thenReturn(previous);
    return item;
  }

  private static UpstreamLineage upstreams(String... envs) {
    DataList upstreams = new DataList();
    for (String env : envs) {
      DataMap upstream = new DataMap();
      upstream.put("dataset", datasetUrn(env));
      upstream.put("type", "TRANSFORMED");
      upstreams.add(upstream);
    }
    DataMap data = new DataMap();
    data.put("upstreams", upstreams);
    return new UpstreamLineage(data);
  }

  private static DataJobInputOutput jobIo(String inputEnv, String outputEnv) {
    DataMap data = new DataMap();
    data.put("inputDatasets", new DataList(List.of(datasetUrn(inputEnv))));
    data.put("outputDatasets", new DataList(List.of(datasetUrn(outputEnv))));
    return new DataJobInputOutput(data);
  }

  private static String datasetUrn(String env) {
    return "urn:li:dataset:(urn:li:dataPlatform:hive,db.up," + env + ")";
  }

  private static DatasetKey datasetKey(String origin) {
    DataMap data = new DataMap();
    data.put("platform", "urn:li:dataPlatform:hive");
    data.put("name", "db.tbl");
    data.put("origin", origin);
    return new DatasetKey(data);
  }

  private static DataFlowInfo dataFlowInfo(String env) {
    DataMap data = new DataMap();
    data.put("name", "flow");
    data.put("env", env);
    return new DataFlowInfo(data);
  }

  private static Deprecation deprecated(String note) {
    return new Deprecation().setDeprecated(true).setNote(note).setActor(ACTOR);
  }
}
