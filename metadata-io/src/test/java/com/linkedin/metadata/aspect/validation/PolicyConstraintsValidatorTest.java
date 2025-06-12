package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PolicyConstraintsValidatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
  private static final TagUrn TEST_TAG_URN = new TagUrn("TestTag");
  private static final TagUrn TEST_TAG_URN_2 = new TagUrn("TestTag2");

  private PolicyConstraintsValidator validator;
  private SearchRetriever mockSearchRetriever;
  private CachingAspectRetriever mockAspectRetriever;
  private GraphRetriever mockGraphRetriever;
  private RetrieverContext retrieverContext;
  private AuthorizationSession mockAuthSession;
  private MockedStatic<AuthUtil> authUtilMockedStatic;
  private final OperationContext operationContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private EntityRegistry entityRegistry = operationContext.getEntityRegistry();
  private ObjectMapper objectMapper = operationContext.getObjectMapper();

  private final AuditStamp auditStamp =
      new AuditStamp().setTime(1000L).setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    validator = new PolicyConstraintsValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(PolicyConstraintsValidator.class.getName())
            .enabled(true)
            .supportedOperations(
                List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(GLOBAL_TAGS_ASPECT_NAME)
                        .build(),
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(SCHEMA_METADATA_ASPECT_NAME)
                        .build(),
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)
                        .build()))
            .build());

    mockSearchRetriever = Mockito.mock(SearchRetriever.class);
    mockGraphRetriever = Mockito.mock(GraphRetriever.class);
    mockAspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    mockAuthSession = Mockito.mock(AuthorizationSession.class);

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .cachingAspectRetriever(mockAspectRetriever)
            .build();
  }

  @AfterMethod
  public void tearDown() {
    if (authUtilMockedStatic != null) {
      authUtilMockedStatic.close();
    }
  }

  @Test
  public void testValidateGlobalTagsWithNoAuthSession() {
    GlobalTags globalTags = createGlobalTags(TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, globalTags, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, null);

    Assert.assertTrue(result.findAny().isPresent());
    AspectValidationException exception =
        validator
            .validateProposedAspectsWithAuth(
                Collections.singletonList(item), retrieverContext, null)
            .findFirst()
            .orElse(null);
    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("No authentication details found"));
  }

  @Test
  public void testValidateGlobalTagsAuthorized() {
    GlobalTags globalTags = createGlobalTags(TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, globalTags, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock authorization to return true
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(TEST_DATASET_URN, GLOBAL_TAGS_ASPECT_NAME))
        .thenReturn(null);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateGlobalTagsUnauthorized() {
    GlobalTags globalTags = createGlobalTags(TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, globalTags, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock authorization to return false
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(TEST_DATASET_URN, GLOBAL_TAGS_ASPECT_NAME))
        .thenReturn(null);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN))))
        .thenReturn(false);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Optional<AspectValidationException> maybeResult = result.findFirst();
    Assert.assertTrue(maybeResult.isPresent());
    AspectValidationException exception = maybeResult.get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(
        exception.getMessage().contains("Unauthorized to modify one or more tag Urns"));
  }

  @Test
  public void testValidateGlobalTagsWithExistingTags() {
    GlobalTags newGlobalTags = createGlobalTags(TEST_TAG_URN, TEST_TAG_URN_2);
    GlobalTags existingGlobalTags = createGlobalTags(TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newGlobalTags, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing tags
    Aspect existingAspect = new Aspect(existingGlobalTags.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(TEST_DATASET_URN, GLOBAL_TAGS_ASPECT_NAME))
        .thenReturn(existingAspect);

    // Only TEST_TAG_URN_2 is being added (difference), TEST_TAG_URN already exists
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN_2))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateGlobalTagsWithTagRemoval() {
    GlobalTags newGlobalTags = createGlobalTags(TEST_TAG_URN);
    GlobalTags existingGlobalTags = createGlobalTags(TEST_TAG_URN, TEST_TAG_URN_2);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newGlobalTags, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing tags
    Aspect existingAspect = new Aspect(existingGlobalTags.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(TEST_DATASET_URN, GLOBAL_TAGS_ASPECT_NAME))
        .thenReturn(existingAspect);

    // TEST_TAG_URN_2 is being removed (difference)
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN_2))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateGlobalTagsNoChanges() {
    GlobalTags newGlobalTags = createGlobalTags(TEST_TAG_URN, TEST_TAG_URN_2);
    GlobalTags existingGlobalTags = createGlobalTags(TEST_TAG_URN, TEST_TAG_URN_2);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newGlobalTags, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing tags
    Aspect existingAspect = new Aspect(existingGlobalTags.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(TEST_DATASET_URN, GLOBAL_TAGS_ASPECT_NAME))
        .thenReturn(existingAspect);

    // No differences, so empty set of subresources
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.emptySet())))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateSchemaMetadataAuthorized() {
    SchemaMetadata schemaMetadata = createSchemaMetadata("field1", TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, schemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(null);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateSchemaMetadataUnauthorized() {
    SchemaMetadata schemaMetadata = createSchemaMetadata("field1", TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, schemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(null);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN))))
        .thenReturn(false);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Optional<AspectValidationException> maybeResult = result.findFirst();
    Assert.assertTrue(maybeResult.isPresent());
    AspectValidationException exception = maybeResult.get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(
        exception.getMessage().contains("Unauthorized to modify one or more tag Urns"));
  }

  @Test
  public void testValidateSchemaMetadataWithExistingTags() {
    SchemaMetadata newSchemaMetadata =
        createSchemaMetadataWithMultipleTags("field1", TEST_TAG_URN, TEST_TAG_URN_2);
    SchemaMetadata existingSchemaMetadata = createSchemaMetadata("field1", TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newSchemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing schema metadata
    Aspect existingAspect = new Aspect(existingSchemaMetadata.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(existingAspect);

    // Only TEST_TAG_URN_2 is being added (difference), TEST_TAG_URN already exists
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN_2))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateSchemaMetadataWithTagRemoval() {
    SchemaMetadata newSchemaMetadata = createSchemaMetadata("field1", TEST_TAG_URN);
    SchemaMetadata existingSchemaMetadata =
        createSchemaMetadataWithMultipleTags("field1", TEST_TAG_URN, TEST_TAG_URN_2);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newSchemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing schema metadata
    Aspect existingAspect = new Aspect(existingSchemaMetadata.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(existingAspect);

    // TEST_TAG_URN_2 is being removed (difference)
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN_2))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateSchemaMetadataNoChanges() {
    SchemaMetadata newSchemaMetadata =
        createSchemaMetadataWithMultipleTags("field1", TEST_TAG_URN, TEST_TAG_URN_2);
    SchemaMetadata existingSchemaMetadata =
        createSchemaMetadataWithMultipleTags("field1", TEST_TAG_URN, TEST_TAG_URN_2);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newSchemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing schema metadata
    Aspect existingAspect = new Aspect(existingSchemaMetadata.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(existingAspect);

    // No differences, so empty set of subresources
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.emptySet())))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateEditableSchemaMetadataAuthorized() {
    EditableSchemaMetadata editableSchemaMetadata =
        createEditableSchemaMetadata("field1", TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, editableSchemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(null);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateEditableSchemaMetadataUnauthorized() {
    EditableSchemaMetadata editableSchemaMetadata =
        createEditableSchemaMetadata("field1", TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, editableSchemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(null);
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN))))
        .thenReturn(false);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Optional<AspectValidationException> maybeResult = result.findFirst();
    Assert.assertTrue(maybeResult.isPresent());
    AspectValidationException exception = maybeResult.get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(
        exception.getMessage().contains("Unauthorized to modify one or more tag Urns"));
  }

  @Test
  public void testValidateEditableSchemaMetadataWithExistingTags() {
    EditableSchemaMetadata newEditableSchemaMetadata =
        createEditableSchemaMetadataWithMultipleTags("field1", TEST_TAG_URN, TEST_TAG_URN_2);
    EditableSchemaMetadata existingEditableSchemaMetadata =
        createEditableSchemaMetadata("field1", TEST_TAG_URN);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newEditableSchemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing editable schema metadata
    Aspect existingAspect = new Aspect(existingEditableSchemaMetadata.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(existingAspect);

    // Only TEST_TAG_URN_2 is being added (difference), TEST_TAG_URN already exists
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN_2))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateEditableSchemaMetadataWithTagRemoval() {
    EditableSchemaMetadata newEditableSchemaMetadata =
        createEditableSchemaMetadata("field1", TEST_TAG_URN);
    EditableSchemaMetadata existingEditableSchemaMetadata =
        createEditableSchemaMetadataWithMultipleTags("field1", TEST_TAG_URN, TEST_TAG_URN_2);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newEditableSchemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing editable schema metadata
    Aspect existingAspect = new Aspect(existingEditableSchemaMetadata.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(existingAspect);

    // TEST_TAG_URN_2 is being removed (difference)
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN_2))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateEditableSchemaMetadataNoChanges() {
    EditableSchemaMetadata newEditableSchemaMetadata =
        createEditableSchemaMetadataWithMultipleTags("field1", TEST_TAG_URN, TEST_TAG_URN_2);
    EditableSchemaMetadata existingEditableSchemaMetadata =
        createEditableSchemaMetadataWithMultipleTags("field1", TEST_TAG_URN, TEST_TAG_URN_2);
    BatchItem item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, newEditableSchemaMetadata, TEST_REGISTRY).stream()
            .findFirst()
            .get();

    // Mock existing editable schema metadata
    Aspect existingAspect = new Aspect(existingEditableSchemaMetadata.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(
                TEST_DATASET_URN, EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
        .thenReturn(existingAspect);

    // No differences, so empty set of subresources
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.emptySet())))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(item), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateUnsupportedAspect() {
    // Create a batch item with an unsupported aspect name
    BatchItem mockItem = Mockito.mock(BatchItem.class);
    Mockito.when(mockItem.getAspectName()).thenReturn("unsupportedAspect");

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(mockItem), retrieverContext, mockAuthSession);

    // Should return empty stream for unsupported aspects (just logs warning)
    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateProposedAspects() {
    // This method should return empty stream as per implementation
    Stream<AspectValidationException> result =
        validator.validateProposedAspects(Collections.emptyList(), retrieverContext);
    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidatePreCommitAspects() {
    // This method should return empty stream as per implementation
    Stream<AspectValidationException> result =
        validator.validatePreCommitAspects(Collections.emptyList(), retrieverContext);
    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateGlobalTagsPatchAuthorized() throws Exception {
    GlobalTags existingGlobalTags = createGlobalTags(TEST_TAG_URN);

    // Create a patch MCP
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_DATASET_URN);
    mcp.setEntityType("dataset");
    mcp.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.PATCH);

    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("add");
    patchOp.setPath("/tags/urn:li:platformResource:my-source/" + TEST_TAG_URN_2);
    patchOp.setValue(
        objectMapper.convertValue(
            Map.of("tag", TEST_TAG_URN_2.toString(), "source", "urn:li:platformResource:my-source"),
            JsonNode.class));
    Map<String, List<String>> arrayPrimaryKeys = new HashMap<>();
    arrayPrimaryKeys.put("tags", List.of("attribution", "source"));

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp))
            .arrayPrimaryKeys(arrayPrimaryKeys)
            .build();
    mcp.setAspect(GenericRecordUtils.serializePatch(genericJsonPatch, objectMapper));

    // Create ProposedItem
    ProposedItem proposedItem = ProposedItem.builder().build(mcp, auditStamp, entityRegistry);

    // Mock existing tags
    Aspect existingAspect = new Aspect(existingGlobalTags.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(TEST_DATASET_URN, GLOBAL_TAGS_ASPECT_NAME))
        .thenReturn(existingAspect);
    Mockito.when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);

    // Mock the patch result to add TEST_TAG_URN_2
    GlobalTags patchedTags = createGlobalTags(TEST_TAG_URN, TEST_TAG_URN_2);
    // You'll need to mock the PatchItemImpl behavior or use a real implementation

    // Only TEST_TAG_URN_2 is being added (difference)
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN_2))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(proposedItem), retrieverContext, mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testValidateGlobalTagsPatchUnauthorized() throws Exception {
    GlobalTags existingGlobalTags = createGlobalTags(TEST_TAG_URN);

    // Create a patch MCP
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_DATASET_URN);
    mcp.setEntityType("dataset");
    mcp.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.PATCH);

    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("add");
    patchOp.setPath("/tags/urn:li:platformResource:my-source/" + TEST_TAG_URN_2);
    patchOp.setValue(
        objectMapper.convertValue(
            Map.of("tag", TEST_TAG_URN_2.toString(), "source", "urn:li:platformResource:my-source"),
            JsonNode.class));
    Map<String, List<String>> arrayPrimaryKeys = new HashMap<>();
    arrayPrimaryKeys.put("tags", List.of("attribution", "source"));

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp))
            .arrayPrimaryKeys(arrayPrimaryKeys)
            .build();
    mcp.setAspect(GenericRecordUtils.serializePatch(genericJsonPatch, objectMapper));

    // Create ProposedItem
    ProposedItem proposedItem = ProposedItem.builder().build(mcp, auditStamp, entityRegistry);

    // Mock existing tags
    Aspect existingAspect = new Aspect(existingGlobalTags.data());
    Mockito.when(
            mockAspectRetriever.getLatestAspectObject(TEST_DATASET_URN, GLOBAL_TAGS_ASPECT_NAME))
        .thenReturn(existingAspect);
    Mockito.when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);

    // Only TEST_TAG_URN_2 is being added (difference) - unauthorized
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                    Mockito.eq(mockAuthSession),
                    Mockito.eq(ApiOperation.UPDATE),
                    Mockito.eq(List.of(TEST_DATASET_URN)),
                    Mockito.eq(Collections.singleton(TEST_TAG_URN_2))))
        .thenReturn(false);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(proposedItem), retrieverContext, mockAuthSession);

    Optional<AspectValidationException> maybeResult = result.findFirst();
    Assert.assertTrue(maybeResult.isPresent());
    AspectValidationException exception = maybeResult.get();
    Assert.assertNotNull(exception);
    Assert.assertTrue(
        exception.getMessage().contains("Unauthorized to modify one or more tag Urns"));
  }

  private GlobalTags createGlobalTags(TagUrn... tagUrns) {
    TagAssociationArray tagArray = new TagAssociationArray();
    for (TagUrn tagUrn : tagUrns) {
      tagArray.add(new TagAssociation().setTag(tagUrn));
    }
    return new GlobalTags().setTags(tagArray);
  }

  private SchemaMetadata createSchemaMetadata(String fieldPath, TagUrn tagUrn) {
    GlobalTags globalTags = createGlobalTags(tagUrn);
    SchemaFieldDataType schemaFieldDataType = new SchemaFieldDataType();
    SchemaFieldDataType.Type type = new SchemaFieldDataType.Type();
    type.setStringType(new StringType());
    schemaFieldDataType.setType(type);
    SchemaField schemaField =
        new SchemaField()
            .setFieldPath(fieldPath)
            .setNativeDataType("string")
            .setType(schemaFieldDataType)
            .setGlobalTags(globalTags);

    SchemaFieldArray fields = new SchemaFieldArray();
    fields.add(schemaField);

    return new SchemaMetadata()
        .setSchemaName("testSchema")
        .setPlatform(new DataPlatformUrn("urn:li:dataPlatform:test"))
        .setVersion(0L)
        .setFields(fields);
  }

  private SchemaMetadata createSchemaMetadataWithMultipleTags(String fieldPath, TagUrn... tagUrns) {
    GlobalTags globalTags = createGlobalTags(tagUrns);
    SchemaFieldDataType schemaFieldDataType = new SchemaFieldDataType();
    SchemaFieldDataType.Type type = new SchemaFieldDataType.Type();
    type.setStringType(new StringType());
    schemaFieldDataType.setType(type);
    SchemaField schemaField =
        new SchemaField()
            .setFieldPath(fieldPath)
            .setNativeDataType("string")
            .setType(schemaFieldDataType)
            .setGlobalTags(globalTags);

    SchemaFieldArray fields = new SchemaFieldArray();
    fields.add(schemaField);

    return new SchemaMetadata()
        .setSchemaName("testSchema")
        .setPlatform(new DataPlatformUrn("urn:li:dataPlatform:test"))
        .setVersion(0L)
        .setFields(fields);
  }

  private EditableSchemaMetadata createEditableSchemaMetadata(String fieldPath, TagUrn tagUrn) {
    GlobalTags globalTags = createGlobalTags(tagUrn);
    EditableSchemaFieldInfo fieldInfo =
        new EditableSchemaFieldInfo().setFieldPath(fieldPath).setGlobalTags(globalTags);

    EditableSchemaFieldInfoArray fieldInfoArray = new EditableSchemaFieldInfoArray();
    fieldInfoArray.add(fieldInfo);

    return new EditableSchemaMetadata().setEditableSchemaFieldInfo(fieldInfoArray);
  }

  private EditableSchemaMetadata createEditableSchemaMetadataWithMultipleTags(
      String fieldPath, TagUrn... tagUrns) {
    GlobalTags globalTags = createGlobalTags(tagUrns);
    EditableSchemaFieldInfo fieldInfo =
        new EditableSchemaFieldInfo().setFieldPath(fieldPath).setGlobalTags(globalTags);

    EditableSchemaFieldInfoArray fieldInfoArray = new EditableSchemaFieldInfoArray();
    fieldInfoArray.add(fieldInfo);

    return new EditableSchemaMetadata().setEditableSchemaFieldInfo(fieldInfoArray);
  }
}
