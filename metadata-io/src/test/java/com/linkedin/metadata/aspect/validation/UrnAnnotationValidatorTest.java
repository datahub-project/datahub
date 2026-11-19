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
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.json.Json;
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

  /**
   * A PATCH carries no resulting aspect at proposal time, so the URNs it adds must be checked once
   * the patch is applied, as for an UPSERT of the same aspect. Uses the existence check: a wrong
   * entity type on this field is already rejected by relationship validation when the patch is
   * applied.
   */
  @Test
  public void testPatchAddingNonExistentUrnIsRejectedLikeUpsert() {
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(UrnAnnotationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build());
    Urn missingType = UrnUtils.getUrn("urn:li:entityType:datahub.notDeployed");
    when(mockAspectRetriever.entityExists(any(OperationFingerprint.class), any()))
        .thenAnswer(
            inv ->
                ((Set<Urn>) inv.getArgument(1))
                    .stream().collect(Collectors.toMap(u -> u, u -> !u.equals(missingType))));

    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:my_prop");
    AuditStamp auditStamp =
        new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"));
    StructuredPropertyDefinition existing =
        new StructuredPropertyDefinition()
            .setQualifiedName("my_prop")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));

    PatchItemImpl patchItem =
        PatchItemImpl.builder()
            .urn(propertyUrn)
            .aspectName(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
            .auditStamp(auditStamp)
            .patch(
                Json.createPatch(
                    Json.createArrayBuilder()
                        .add(
                            Json.createObjectBuilder()
                                .add("op", "add")
                                .add("path", "/entityTypes/" + missingType)
                                .add("value", missingType.toString()))
                        .build()))
            .build(TEST_CONTEXT.getEntityRegistry());
    ChangeItemImpl patched = patchItem.applyPatch(existing, TEST_CONTEXT.getAspectRetriever());

    ChangeItemImpl upsert =
        ChangeItemImpl.builder()
            .urn(propertyUrn)
            .aspectName(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
            .recordTemplate(patched.getRecordTemplate())
            .auditStamp(auditStamp)
            .build(TEST_CONTEXT.getAspectRetriever());
    assertFalse(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY, List.of(upsert), mockRetrieverContext, null)
            .collect(Collectors.toList())
            .isEmpty(),
        "UPSERT with a non-existent entity type is rejected");
    assertTrue(
        validator
            .validatePreCommit(
                OperationFingerprint.EMPTY, List.<ChangeMCP>of(upsert), mockRetrieverContext)
            .collect(Collectors.toList())
            .isEmpty(),
        "UPSERT is not validated a second time at pre-commit");

    List<AspectValidationException> patchExceptions =
        Stream.concat(
                validator.validateProposed(
                    OperationFingerprint.EMPTY, List.of(patchItem), mockRetrieverContext, null),
                validator.validatePreCommit(
                    OperationFingerprint.EMPTY, List.<ChangeMCP>of(patched), mockRetrieverContext))
            .collect(Collectors.toList());
    assertEquals(patchExceptions.size(), 1, "PATCH with a non-existent entity type is rejected");
  }
}
