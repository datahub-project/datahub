package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.UrnValidationFieldSpec;
import com.linkedin.metadata.models.annotation.UrnValidationAnnotation;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UrnAnnotationValidatorTest {

  private static final OperationContext TEST_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(UrnAnnotationValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("UPSERT"))
          .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
          .build();

  @Mock private AspectSpec mockAspectSpec;

  @Mock private BatchItem mockBatchItem;

  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private AspectRetriever mockAspectRetriever;

  @Mock private RecordTemplate mockRecordTemplate;

  private UrnAnnotationValidator validator;

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    validator = new UrnAnnotationValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_CONTEXT.getEntityRegistry());
  }

  @Test
  public void testValidateProposedAspects_WithStrictValidation() throws Exception {
    // Arrange
    Urn validUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

    // Create DataMap with URN
    DataMap dataMap = new DataMap();
    dataMap.put("urn", validUrn.toString());

    // Set up mock UrnValidationAnnotation
    UrnValidationAnnotation annotation = mock(UrnValidationAnnotation.class);
    when(annotation.isStrict()).thenReturn(true);
    when(annotation.getEntityTypes()).thenReturn(Collections.singletonList("dataset"));

    // Set up UrnValidationFieldSpec
    UrnValidationFieldSpec fieldSpec = mock(UrnValidationFieldSpec.class);
    when(fieldSpec.getUrnValidationAnnotation()).thenReturn(annotation);

    // Set up AspectSpec's UrnValidationFieldSpecMap
    Map<String, UrnValidationFieldSpec> fieldSpecMap = new HashMap<>();
    fieldSpecMap.put("/urn", fieldSpec);
    when(mockAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(fieldSpecMap);

    // Set up BatchItem mocks
    when(mockBatchItem.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockBatchItem.getRecordTemplate()).thenReturn(mockRecordTemplate);
    when(mockRecordTemplate.data()).thenReturn(dataMap);

    // Set up empty existence map for the strict validation test
    when(mockAspectRetriever.entityExists(any(OperationFingerprint.class), any()))
        .thenReturn(Collections.emptyMap());

    // Act
    Stream<AspectValidationException> result =
        validator.validateProposedAspects(
            OperationFingerprint.EMPTY,
            Collections.singletonList(mockBatchItem),
            mockRetrieverContext);

    // Assert
    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertTrue(exceptions.isEmpty(), "No validation exceptions should be thrown for valid URN");
  }

  @Test
  public void testValidateProposedAspects_WithFailedStrictValidation() throws Exception {
    // Arrange
    String invalidUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD";

    // Create DataMap with URN
    DataMap dataMap = new DataMap();
    dataMap.put("urn", invalidUrn);

    // Set up mock UrnValidationAnnotation
    UrnValidationAnnotation annotation = mock(UrnValidationAnnotation.class);
    when(annotation.isStrict()).thenReturn(true);
    when(annotation.getEntityTypes()).thenReturn(Collections.singletonList("dataset"));

    // Set up UrnValidationFieldSpec
    UrnValidationFieldSpec fieldSpec = mock(UrnValidationFieldSpec.class);
    when(fieldSpec.getUrnValidationAnnotation()).thenReturn(annotation);

    // Set up AspectSpec's UrnValidationFieldSpecMap
    Map<String, UrnValidationFieldSpec> fieldSpecMap = new HashMap<>();
    fieldSpecMap.put("/urn", fieldSpec);
    when(mockAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(fieldSpecMap);

    // Set up BatchItem mocks
    when(mockBatchItem.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockBatchItem.getRecordTemplate()).thenReturn(mockRecordTemplate);
    when(mockRecordTemplate.data()).thenReturn(dataMap);

    // Set up empty existence map for the strict validation test
    when(mockAspectRetriever.entityExists(any(OperationFingerprint.class), any()))
        .thenReturn(Collections.emptyMap());

    // Act
    Stream<AspectValidationException> result =
        validator.validateProposedAspects(
            OperationFingerprint.EMPTY,
            Collections.singletonList(mockBatchItem),
            mockRetrieverContext);

    // Assert
    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertFalse(exceptions.isEmpty(), "Validation exception should be thrown for invalid URN");
    String message = exceptions.get(0).getMessage();
    assertTrue(message.contains("Invalid URN at path /urn"), message);
    assertTrue(message.contains("expected a dataset URN"), message);
    assertTrue(message.contains(invalidUrn), message);
  }

  @Test
  public void testValidateProposedAspects_WithBareStringField() {
    String bareField = "segment_denial_rate";
    setupSingleUrnField("field", bareField, true, Collections.singletonList("schemaField"));

    List<AspectValidationException> exceptions = runValidation();

    assertFalse(
        exceptions.isEmpty(), "Validation exception should be thrown for bare string field");
    String message = exceptions.get(0).getMessage();
    assertTrue(message.contains("Invalid URN at path /field"), message);
    assertTrue(
        message.contains("expected a schemaField URN (urn:li:schemaField:(<dataset>,<column>))"),
        message);
    assertTrue(message.contains(bareField), message);
    assertFalse(message.contains("Failed to retrieve entity"), message);
  }

  @Test
  public void testValidateProposedAspects_WithMultipleAllowedEntityTypes() {
    setupSingleUrnField("entity", "not-a-urn", true, List.of("dataset", "dataJob"));

    List<AspectValidationException> exceptions = runValidation();

    assertFalse(exceptions.isEmpty(), "Validation exception should be thrown for invalid URN");
    String message = exceptions.get(0).getMessage();
    assertTrue(message.contains("Invalid URN at path /entity"), message);
    assertTrue(message.contains("[dataset, dataJob]"), message);
  }

  @Test
  public void testValidateProposedAspects_WithoutDeclaredEntityTypes() {
    setupSingleUrnField("entity", "not-a-urn", true, Collections.emptyList());

    List<AspectValidationException> exceptions = runValidation();

    assertFalse(exceptions.isEmpty(), "Validation exception should be thrown for invalid URN");
    String message = exceptions.get(0).getMessage();
    assertTrue(message.contains("Invalid URN at path /entity"), message);
    assertTrue(message.contains("expected a valid URN"), message);
  }

  @Test
  public void testValidateProposedAspects_WithParseableUrnRejectedByStrictValidation() {
    // Commas are not permitted in non-tuple URNs, so this parses but fails strict validation.
    setupSingleUrnField(
        "urn", "urn:li:corpuser:foo,bar", true, Collections.singletonList("corpuser"));

    List<AspectValidationException> exceptions = runValidation();

    assertFalse(exceptions.isEmpty(), "Validation exception should be thrown for illegal URN");
    String message = exceptions.get(0).getMessage();
    assertTrue(message.contains("Invalid URN at path /urn"), message);
    assertTrue(message.contains("comma"), message);
  }

  @Test
  public void testValidateProposedAspects_WithInvalidEntityType() throws Exception {
    // Arrange
    Urn invalidUrn = Urn.createFromString("urn:li:corpuser:johndoe");

    // Create DataMap with URN
    DataMap dataMap = new DataMap();
    dataMap.put("urn", invalidUrn.toString());

    // Set up mock UrnValidationAnnotation
    UrnValidationAnnotation annotation = mock(UrnValidationAnnotation.class);
    when(annotation.isStrict()).thenReturn(true);
    when(annotation.getEntityTypes()).thenReturn(Collections.singletonList("dataset"));

    // Set up UrnValidationFieldSpec
    UrnValidationFieldSpec fieldSpec = mock(UrnValidationFieldSpec.class);
    when(fieldSpec.getUrnValidationAnnotation()).thenReturn(annotation);

    // Set up AspectSpec's UrnValidationFieldSpecMap
    Map<String, UrnValidationFieldSpec> fieldSpecMap = new HashMap<>();
    fieldSpecMap.put("/urn", fieldSpec);
    when(mockAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(fieldSpecMap);

    // Set up BatchItem mocks
    when(mockBatchItem.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockBatchItem.getRecordTemplate()).thenReturn(mockRecordTemplate);
    when(mockRecordTemplate.data()).thenReturn(dataMap);

    // Set up empty existence map
    when(mockAspectRetriever.entityExists(any(OperationFingerprint.class), any()))
        .thenReturn(Collections.emptyMap());

    // Act
    Stream<AspectValidationException> result =
        validator.validateProposedAspects(
            OperationFingerprint.EMPTY,
            Collections.singletonList(mockBatchItem),
            mockRetrieverContext);

    // Assert
    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertFalse(
        exceptions.isEmpty(), "Validation exception should be thrown for invalid entity type");
    String message = exceptions.get(0).getMessage();
    assertTrue(message.contains("Invalid URN entity type at path /urn"), message);
    assertTrue(message.contains("expected one of [dataset]"), message);
    assertTrue(message.contains("got corpuser"), message);
    assertTrue(message.contains(invalidUrn.toString()), message);
  }

  @Test
  public void testValidateProposedAspects_EntityTypeMismatchStoredByNewerSchemaIsAccepted()
      throws Exception {
    // A newer version can allow an entity type this version's annotation doesn't (read after a
    // rollback). A urn it already stored is kept on rewrite instead of failing the write; only
    // newly added urns are validated.
    assertTrue(validateRewriteOfStoredMismatch(2L).isEmpty());
  }

  @Test
  public void testValidateProposedAspects_EntityTypeMismatchStoredBySameSchemaIsRejected()
      throws Exception {
    // Invalid data this version (or an older one) wrote is not grandfathered in on re-save.
    assertEquals(validateRewriteOfStoredMismatch(1L).size(), 1);
  }

  private List<AspectValidationException> validateRewriteOfStoredMismatch(long storedSchemaVersion)
      throws Exception {
    Urn storedUrn = Urn.createFromString("urn:li:corpuser:johndoe");
    DataMap dataMap = new DataMap();
    dataMap.put("urn", storedUrn.toString());

    UrnValidationAnnotation annotation = mock(UrnValidationAnnotation.class);
    when(annotation.getEntityTypes()).thenReturn(Collections.singletonList("dataset"));
    UrnValidationFieldSpec fieldSpec = mock(UrnValidationFieldSpec.class);
    when(fieldSpec.getUrnValidationAnnotation()).thenReturn(annotation);
    Map<String, UrnValidationFieldSpec> fieldSpecMap = new HashMap<>();
    fieldSpecMap.put("/urn", fieldSpec);
    when(mockAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(fieldSpecMap);
    when(mockAspectSpec.getSchemaVersion()).thenReturn(1L);

    Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,Stored,PROD)");
    when(mockBatchItem.getUrn()).thenReturn(entityUrn);
    when(mockBatchItem.getAspectName()).thenReturn("someAspect");
    when(mockBatchItem.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockBatchItem.getRecordTemplate()).thenReturn(mockRecordTemplate);
    when(mockRecordTemplate.data()).thenReturn(dataMap);
    DataMap storedData = new DataMap();
    storedData.put("urn", storedUrn.toString());
    RecordTemplate storedRecord = mock(RecordTemplate.class);
    when(storedRecord.data()).thenReturn(storedData);
    SystemAspect stored = mock(SystemAspect.class);
    when(stored.getRecordTemplate()).thenReturn(storedRecord);
    when(stored.getSystemMetadata())
        .thenReturn(new SystemMetadata().setSchemaVersion(storedSchemaVersion));
    when(mockAspectRetriever.getLatestSystemAspect(
            any(OperationFingerprint.class), eq(entityUrn), eq("someAspect")))
        .thenReturn(stored);
    when(mockAspectRetriever.entityExists(any(OperationFingerprint.class), any()))
        .thenReturn(Collections.emptyMap());

    return validator
        .validateProposedAspects(
            OperationFingerprint.EMPTY,
            Collections.singletonList(mockBatchItem),
            mockRetrieverContext)
        .collect(Collectors.toList());
  }

  @Test
  public void testValidateProposedAspects_WithExistenceCheck() throws Exception {
    // Arrange
    Urn existingUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,ExistingDataset,PROD)");

    // Create DataMap with URN
    DataMap dataMap = new DataMap();
    dataMap.put("urn", existingUrn.toString());

    // Set up mock UrnValidationAnnotation
    UrnValidationAnnotation annotation = mock(UrnValidationAnnotation.class);
    when(annotation.isExist()).thenReturn(true);
    when(annotation.getEntityTypes()).thenReturn(Collections.emptyList());

    // Set up UrnValidationFieldSpec
    UrnValidationFieldSpec fieldSpec = mock(UrnValidationFieldSpec.class);
    when(fieldSpec.getUrnValidationAnnotation()).thenReturn(annotation);

    // Set up AspectSpec's UrnValidationFieldSpecMap
    Map<String, UrnValidationFieldSpec> fieldSpecMap = new HashMap<>();
    fieldSpecMap.put("/urn", fieldSpec);
    when(mockAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(fieldSpecMap);

    // Set up BatchItem mocks
    when(mockBatchItem.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockBatchItem.getRecordTemplate()).thenReturn(mockRecordTemplate);
    when(mockRecordTemplate.data()).thenReturn(dataMap);

    Map<Urn, Boolean> existenceMap = new HashMap<>();
    existenceMap.put(existingUrn, true);
    when(mockAspectRetriever.entityExists(
            OperationFingerprint.EMPTY, Collections.singleton(existingUrn)))
        .thenReturn(existenceMap);

    // Act
    Stream<AspectValidationException> result =
        validator.validateProposedAspects(
            OperationFingerprint.EMPTY,
            Collections.singletonList(mockBatchItem),
            mockRetrieverContext);

    // Assert
    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertTrue(exceptions.isEmpty(), "No validation exceptions should be thrown for existing URN");
  }

  @Test
  public void testValidateProposedAspects_WithNonExistentUrn() throws Exception {
    // Arrange
    Urn nonExistentUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,NonExistentDataset,PROD)");

    // Create DataMap with URN
    DataMap dataMap = new DataMap();
    dataMap.put("urn", nonExistentUrn.toString());

    // Set up mock UrnValidationAnnotation
    UrnValidationAnnotation annotation = mock(UrnValidationAnnotation.class);
    when(annotation.isExist()).thenReturn(true);
    when(annotation.getEntityTypes()).thenReturn(Collections.emptyList());

    // Set up UrnValidationFieldSpec
    UrnValidationFieldSpec fieldSpec = mock(UrnValidationFieldSpec.class);
    when(fieldSpec.getUrnValidationAnnotation()).thenReturn(annotation);

    // Set up AspectSpec's UrnValidationFieldSpecMap
    Map<String, UrnValidationFieldSpec> fieldSpecMap = new HashMap<>();
    fieldSpecMap.put("/urn", fieldSpec);
    when(mockAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(fieldSpecMap);

    // Set up BatchItem mocks
    when(mockBatchItem.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockBatchItem.getRecordTemplate()).thenReturn(mockRecordTemplate);
    when(mockRecordTemplate.data()).thenReturn(dataMap);

    Map<Urn, Boolean> existenceMap = new HashMap<>();
    existenceMap.put(nonExistentUrn, false);
    when(mockAspectRetriever.entityExists(
            OperationFingerprint.EMPTY, Collections.singleton(nonExistentUrn)))
        .thenReturn(existenceMap);

    // Act
    Stream<AspectValidationException> result =
        validator.validateProposedAspects(
            OperationFingerprint.EMPTY,
            Collections.singletonList(mockBatchItem),
            mockRetrieverContext);

    // Assert
    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertFalse(exceptions.isEmpty(), "Validation exception should be thrown for non-existent URN");
    assertTrue(exceptions.get(0).getMessage().contains("Urn does not exist"));
  }

  private void setupSingleUrnField(
      String fieldName, String value, boolean strict, List<String> entityTypes) {
    DataMap dataMap = new DataMap();
    dataMap.put(fieldName, value);

    UrnValidationAnnotation annotation = mock(UrnValidationAnnotation.class);
    when(annotation.isStrict()).thenReturn(strict);
    when(annotation.getEntityTypes()).thenReturn(entityTypes);

    UrnValidationFieldSpec fieldSpec = mock(UrnValidationFieldSpec.class);
    when(fieldSpec.getUrnValidationAnnotation()).thenReturn(annotation);

    when(mockAspectSpec.getUrnValidationFieldSpecMap())
        .thenReturn(Map.of("/" + fieldName, fieldSpec));
    when(mockBatchItem.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockBatchItem.getRecordTemplate()).thenReturn(mockRecordTemplate);
    when(mockRecordTemplate.data()).thenReturn(dataMap);
    when(mockAspectRetriever.entityExists(any(OperationFingerprint.class), any()))
        .thenReturn(Collections.emptyMap());
  }

  private List<AspectValidationException> runValidation() {
    return validator
        .validateProposedAspects(
            OperationFingerprint.EMPTY,
            Collections.singletonList(mockBatchItem),
            mockRetrieverContext)
        .collect(Collectors.toList());
  }

  private static final Urn PROPERTY_URN = UrnUtils.getUrn("urn:li:structuredProperty:my_prop");
  private static final Urn EXISTING_PLATFORM = UrnUtils.getUrn("urn:li:dataPlatform:existing");
  private static final Urn MISSING_PLATFORM = UrnUtils.getUrn("urn:li:dataPlatform:missing");
  private static final Urn DELETED_PLATFORM = UrnUtils.getUrn("urn:li:dataPlatform:deleted");

  @Test
  public void testPatchAddingNonExistentUrnIsRejected() {
    ChangeItemImpl patched =
        patchDefinition(
            definition(), addOp("/allowedPlatforms/" + MISSING_PLATFORM, MISSING_PLATFORM));

    List<AspectValidationException> exceptions = runPreCommit(patched);

    assertEquals(exceptions.size(), 1);
    assertTrue(exceptions.get(0).getMessage().contains(MISSING_PLATFORM.toString()));
  }

  @Test
  public void testPatchAddingWrongEntityTypeIsRejected() {
    Urn user = UrnUtils.getUrn("urn:li:corpuser:not_a_platform");
    ChangeItemImpl patched =
        patchDefinition(definition(), addOp("/allowedPlatforms/" + user, user));

    List<AspectValidationException> exceptions = runPreCommit(patched);

    assertEquals(exceptions.size(), 1);
    assertTrue(exceptions.get(0).getMessage().contains("dataPlatform"));
  }

  /** A stored URN whose entity was deleted must not block edits that leave it untouched. */
  @Test
  public void testPatchKeepingStaleStoredUrnIsAccepted() {
    StructuredPropertyDefinition stored =
        definition().setAllowedPlatforms(new UrnArray(DELETED_PLATFORM));

    ChangeItemImpl descriptionEdit =
        patchDefinition(
            stored,
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/description")
                        .add("value", "new description")));
    assertTrue(runPreCommit(descriptionEdit).isEmpty());

    ChangeItemImpl addsValidPlatform =
        patchDefinition(stored, addOp("/allowedPlatforms/" + EXISTING_PLATFORM, EXISTING_PLATFORM));
    assertTrue(runPreCommit(addsValidPlatform).isEmpty());
    verify(mockAspectRetriever, never())
        .entityExists(
            any(OperationFingerprint.class), argThat(urns -> urns.contains(DELETED_PLATFORM)));
  }

  @Test
  public void testPatchAddingValidUrnIsAccepted() {
    ChangeItemImpl patched =
        patchDefinition(
            definition(), addOp("/allowedPlatforms/" + EXISTING_PLATFORM, EXISTING_PLATFORM));

    assertTrue(runPreCommit(patched).isEmpty());
  }

  /** UPSERTs were checked as proposals; pre-commit doesn't check them again. */
  @Test
  public void testUpsertIsNotValidatedAgainAtPreCommit() {
    ChangeItemImpl upsert =
        ChangeItemImpl.builder()
            .urn(PROPERTY_URN)
            .aspectName(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
            .recordTemplate(definition().setAllowedPlatforms(new UrnArray(MISSING_PLATFORM)))
            .auditStamp(auditStamp())
            .build(TEST_CONTEXT.getAspectRetriever());

    assertTrue(runPreCommit(upsert).isEmpty());
  }

  @Test
  public void testPatchOnAspectWithoutUrnValidationFieldsIsSkipped() {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(PROPERTY_URN);
    when(item.getAspectName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getMetadataChangeProposal())
        .thenReturn(new MetadataChangeProposal().setChangeType(ChangeType.PATCH));
    when(item.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(Collections.emptyMap());

    assertTrue(runPreCommit(item).isEmpty());
    verify(item, never()).getRecordTemplate();
    verify(item, never()).getPreviousRecordTemplate();
    verify(mockAspectRetriever, never()).entityExists(any(OperationFingerprint.class), any());
  }

  private List<AspectValidationException> runPreCommit(ChangeMCP item) {
    when(mockAspectRetriever.entityExists(any(OperationFingerprint.class), any()))
        .thenAnswer(
            inv ->
                ((Set<Urn>) inv.getArgument(1))
                    .stream()
                        .collect(
                            Collectors.toMap(
                                u -> u,
                                u -> !u.equals(MISSING_PLATFORM) && !u.equals(DELETED_PLATFORM))));
    return validator
        .validatePreCommit(OperationFingerprint.EMPTY, List.of(item), mockRetrieverContext)
        .collect(Collectors.toList());
  }

  private static StructuredPropertyDefinition definition() {
    return new StructuredPropertyDefinition()
        .setQualifiedName("my_prop")
        .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
        .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
  }

  private static JsonArrayBuilder addOp(String path, Urn value) {
    return Json.createArrayBuilder()
        .add(
            Json.createObjectBuilder()
                .add("op", "add")
                .add("path", path)
                .add("value", value.toString()));
  }

  /** Applies the patch to {@code stored} as the write path does, with it as the previous value. */
  private static ChangeItemImpl patchDefinition(
      StructuredPropertyDefinition stored, JsonArrayBuilder ops) {
    ChangeItemImpl patched =
        PatchItemImpl.builder()
            .urn(PROPERTY_URN)
            .aspectName(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
            .auditStamp(auditStamp())
            .patch(Json.createPatch(ops.build()))
            .build(TEST_CONTEXT.getEntityRegistry())
            .applyPatch(stored, TEST_CONTEXT.getAspectRetriever());
    SystemAspect previous = mock(SystemAspect.class);
    when(previous.getRecordTemplate()).thenReturn(stored);
    patched.setPreviousSystemAspect(previous);
    return patched;
  }

  private static AuditStamp auditStamp() {
    return new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"));
  }
}
