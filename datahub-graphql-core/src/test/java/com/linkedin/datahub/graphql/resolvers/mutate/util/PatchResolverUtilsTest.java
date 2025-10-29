package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.ArrayPrimaryKeyInput;
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchOperationInput;
import com.linkedin.datahub.graphql.generated.PatchOperationType;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.SystemMetadataInput;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PatchResolverUtilsTest {

  private QueryContext _context;
  private OperationContext _operationContext;
  private ObjectMapper _objectMapper;
  private EntityRegistry _entityRegistry;

  @BeforeMethod
  public void setup() {
    _context = TestUtils.getMockAllowContext("urn:li:corpuser:test-user");
    _operationContext = _context.getOperationContext();
    _objectMapper = new ObjectMapper();
    _entityRegistry = mock(EntityRegistry.class);
  }

  // ==================== resolveEntityUrn() Tests ====================

  @Test
  public void testResolveEntityUrnSuccess() throws Exception {
    String urn = "urn:li:glossaryTerm:test-term";
    Urn resolvedUrn = PatchResolverUtils.resolveEntityUrn(urn, "glossaryTerm");
    assertNotNull(resolvedUrn);
    assertEquals(resolvedUrn.toString(), urn);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testResolveEntityUrnWithEmptyString() throws Exception {
    PatchResolverUtils.resolveEntityUrn("", "glossaryTerm");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testResolveEntityUrnWithNull() throws Exception {
    PatchResolverUtils.resolveEntityUrn(null, "glossaryTerm");
  }

  // ==================== extractEntityName() Tests ====================

  @Test
  public void testExtractEntityNameSuccess() {
    List<PatchOperationInput> operations =
        Arrays.asList(
            createPatchOperation(PatchOperationType.ADD, "/name", "\"Test Name\""),
            createPatchOperation(PatchOperationType.ADD, "/definition", "\"Test definition\""));
    String name = PatchResolverUtils.extractEntityName(operations);
    assertEquals(name, "\"Test Name\"");
  }

  @Test
  public void testExtractEntityNameNotFound() {
    List<PatchOperationInput> operations =
        Arrays.asList(
            createPatchOperation(PatchOperationType.ADD, "/definition", "\"Test definition\""),
            createPatchOperation(PatchOperationType.ADD, "/termSource", "\"Internal\""));
    String name = PatchResolverUtils.extractEntityName(operations);
    assertNull(name);
  }

  @Test
  public void testExtractEntityNameWithNullValue() {
    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/name", null));
    String name = PatchResolverUtils.extractEntityName(operations);
    assertNull(name);
  }

  @Test
  public void testExtractEntityNameEmptyOperations() {
    String name = PatchResolverUtils.extractEntityName(new ArrayList<>());
    assertNull(name);
  }

  // ==================== validateNameForEntityType() Tests ====================

  @Test
  public void testValidateNameForEntityTypeWithName() {
    PatchResolverUtils.validateNameForEntityType("glossaryTerm", "Test Name");
  }

  @Test
  public void testValidateNameForEntityTypeWithNameForNode() {
    PatchResolverUtils.validateNameForEntityType("glossaryNode", "Test Name");
  }

  @Test
  public void testValidateNameForEntityTypeWithoutNameForTerm() {
    PatchResolverUtils.validateNameForEntityType("glossaryTerm", null);
  }

  @Test
  public void testValidateNameForEntityTypeWithoutNameForNode() {
    PatchResolverUtils.validateNameForEntityType("glossaryNode", null);
  }

  @Test
  public void testValidateNameForEntityTypeOtherEntity() {
    PatchResolverUtils.validateNameForEntityType("dataset", null);
  }

  @Test
  public void testValidateNameForEntityTypeNullEntityType() {
    PatchResolverUtils.validateNameForEntityType(null, null);
  }

  // ==================== createPatchAspect() Tests ====================

  @Test
  public void testCreatePatchAspectBasic() throws Exception {
    List<PatchOperationInput> operations =
        Arrays.asList(
            createPatchOperation(PatchOperationType.ADD, "/name", "\"Test Term\""),
            createPatchOperation(PatchOperationType.ADD, "/definition", "\"Test definition\""));

    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);
    assertNotNull(aspect);
    assertNotNull(aspect.getValue());
  }

  @Test
  public void testCreatePatchAspectWithArrayPrimaryKeys() throws Exception {
    List<PatchOperationInput> operations =
        Arrays.asList(
            createPatchOperation(PatchOperationType.ADD, "/owners/0/id", "urn:li:corpuser:test"));

    List<ArrayPrimaryKeyInput> arrayPrimaryKeys =
        Arrays.asList(createArrayPrimaryKeyInput("owners", Arrays.asList("id")));

    var aspect = PatchResolverUtils.createPatchAspect(operations, arrayPrimaryKeys, null, _context);
    assertNotNull(aspect);
  }

  @Test
  public void testCreatePatchAspectWithForceGenericPatch() throws Exception {
    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/name", "\"Test Term\""));

    var aspect = PatchResolverUtils.createPatchAspect(operations, null, true, _context);
    assertNotNull(aspect);
  }

  @Test
  public void testCreatePatchAspectWithReplaceOperation() throws Exception {
    List<PatchOperationInput> operations =
        Arrays.asList(
            createPatchOperation(PatchOperationType.REPLACE, "/name", "\"Updated Term\""));

    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);
    assertNotNull(aspect);
  }

  @Test
  public void testCreatePatchAspectWithRemoveOperation() throws Exception {
    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.REMOVE, "/oldField", null));

    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);
    assertNotNull(aspect);
  }

  @Test
  public void testCreatePatchAspectWithEmptyStringValue() throws Exception {
    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/name", "\"\""));

    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);
    assertNotNull(aspect);
  }

  @Test
  public void testCreatePatchAspectWithNullValue() throws Exception {
    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/optionalField", null));

    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);
    assertNotNull(aspect);
  }

  // ==================== createMetadataChangeProposal() Tests ====================

  @Test
  public void testCreateMetadataChangeProposalWithoutSystemMetadata() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:glossaryTerm:test");
    String aspectName = "glossaryTermInfo";

    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/name", "\"Test\""));
    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);

    var mcp =
        PatchResolverUtils.createMetadataChangeProposal(entityUrn, aspectName, aspect, null, null);

    assertNotNull(mcp);
    assertEquals(mcp.getEntityUrn(), entityUrn);
    assertEquals(mcp.getAspectName(), aspectName);
    assertNotNull(mcp.getAspect());
  }

  @Test
  public void testCreateMetadataChangeProposalWithSystemMetadata() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:glossaryTerm:test");
    String aspectName = "glossaryTermInfo";

    SystemMetadataInput systemMetadataInput = new SystemMetadataInput();
    systemMetadataInput.setRunId("test-run-id");
    systemMetadataInput.setLastObserved(System.currentTimeMillis());

    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/name", "\"Test\""));
    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);

    var mcp =
        PatchResolverUtils.createMetadataChangeProposal(
            entityUrn, aspectName, aspect, systemMetadataInput, null);

    assertNotNull(mcp);
    assertNotNull(mcp.getSystemMetadata());
  }

  @Test
  public void testCreateMetadataChangeProposalWithProperties() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:glossaryTerm:test");
    String aspectName = "glossaryTermInfo";

    SystemMetadataInput systemMetadataInput = new SystemMetadataInput();
    StringMapEntryInput property1 = new StringMapEntryInput();
    property1.setKey("key1");
    property1.setValue("value1");
    StringMapEntryInput property2 = new StringMapEntryInput();
    property2.setKey("key2");
    property2.setValue("value2");
    systemMetadataInput.setProperties(Arrays.asList(property1, property2));

    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/name", "\"Test\""));
    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);

    var mcp =
        PatchResolverUtils.createMetadataChangeProposal(
            entityUrn, aspectName, aspect, systemMetadataInput, null);

    assertNotNull(mcp);
    assertNotNull(mcp.getSystemMetadata());
    assertNotNull(mcp.getSystemMetadata().getProperties());
  }

  @Test
  public void testCreateMetadataChangeProposalWithHeaders() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:glossaryTerm:test");
    String aspectName = "glossaryTermInfo";

    StringMapEntryInput header1 = new StringMapEntryInput();
    header1.setKey("h1");
    header1.setValue("v1");
    StringMapEntryInput header2 = new StringMapEntryInput();
    header2.setKey("h2");
    header2.setValue("v2");
    List<StringMapEntryInput> headers = Arrays.asList(header1, header2);

    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/name", "\"Test\""));
    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);

    var mcp =
        PatchResolverUtils.createMetadataChangeProposal(
            entityUrn, aspectName, aspect, null, headers);

    assertNotNull(mcp);
    assertNotNull(mcp.getHeaders());
  }

  @Test
  public void testCreateMetadataChangeProposalWithAllParameters() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:glossaryTerm:test");
    String aspectName = "glossaryTermInfo";

    SystemMetadataInput systemMetadataInput = new SystemMetadataInput();
    systemMetadataInput.setRunId("test-run");

    StringMapEntryInput header = new StringMapEntryInput();
    header.setKey("test-header");
    header.setValue("test-value");
    List<StringMapEntryInput> headers = Arrays.asList(header);

    List<PatchOperationInput> operations =
        Arrays.asList(createPatchOperation(PatchOperationType.ADD, "/name", "\"Test\""));
    var aspect = PatchResolverUtils.createPatchAspect(operations, null, null, _context);

    var mcp =
        PatchResolverUtils.createMetadataChangeProposal(
            entityUrn, aspectName, aspect, systemMetadataInput, headers);

    assertNotNull(mcp);
    assertNotNull(mcp.getSystemMetadata());
    assertNotNull(mcp.getHeaders());
  }

  // ==================== createPatchEntitiesMcps() Tests ====================

  @Test
  public void testCreatePatchEntitiesMcpsSuccess() throws Exception {
    PatchEntityInput input = createGlossaryTermInput("urn:li:glossaryTerm:test", "Test Term");

    setupEntityRegistryMocks();

    List<PatchEntityInput> inputs = Arrays.asList(input);
    List<com.linkedin.mxe.MetadataChangeProposal> mcps =
        PatchResolverUtils.createPatchEntitiesMcps(inputs, _context, _entityRegistry);

    assertNotNull(mcps);
    assertEquals(mcps.size(), 1);
  }

  @Test
  public void testCreatePatchEntitiesMcpsBatch() throws Exception {
    PatchEntityInput input1 = createGlossaryTermInput("urn:li:glossaryTerm:test1", "Term 1");
    PatchEntityInput input2 = createGlossaryTermInput("urn:li:glossaryTerm:test2", "Term 2");

    setupEntityRegistryMocks();

    List<PatchEntityInput> inputs = Arrays.asList(input1, input2);
    List<com.linkedin.mxe.MetadataChangeProposal> mcps =
        PatchResolverUtils.createPatchEntitiesMcps(inputs, _context, _entityRegistry);

    assertNotNull(mcps);
    assertEquals(mcps.size(), 2);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Aspect.*not found.*")
  public void testCreatePatchEntitiesMcpsInvalidAspect() throws Exception {
    PatchEntityInput input = createGlossaryTermInput("urn:li:glossaryTerm:test", "Test Term");
    input.setAspectName("nonExistentAspect");

    EntitySpec entitySpec = mock(EntitySpec.class);
    when(_entityRegistry.getEntitySpec("glossaryTerm")).thenReturn(entitySpec);
    when(entitySpec.getAspectSpec("nonExistentAspect")).thenReturn(null);

    List<PatchEntityInput> inputs = Arrays.asList(input);
    PatchResolverUtils.createPatchEntitiesMcps(inputs, _context, _entityRegistry);
  }

  // ==================== checkBatchAuthorization() Tests ====================

  @Test
  public void testCheckBatchAuthorizationSuccess() {
    PatchEntityInput input1 = createGlossaryTermInput("urn:li:glossaryTerm:test1", "Term 1");
    PatchEntityInput input2 = createGlossaryTermInput("urn:li:glossaryTerm:test2", "Term 2");

    List<PatchEntityInput> inputs = Arrays.asList(input1, input2);

    PatchResolverUtils.checkBatchAuthorization(inputs, _context);
  }

  @Test(expectedExceptions = com.linkedin.datahub.graphql.exception.AuthorizationException.class)
  public void testCheckBatchAuthorizationFailure() {
    PatchEntityInput input = createGlossaryTermInput("urn:li:glossaryTerm:test", "Term");

    QueryContext denyContext = TestUtils.getMockDenyContext("urn:li:corpuser:test-user");
    List<PatchEntityInput> inputs = Arrays.asList(input);

    PatchResolverUtils.checkBatchAuthorization(inputs, denyContext);
  }

  @Test(expectedExceptions = com.linkedin.datahub.graphql.exception.AuthorizationException.class)
  public void testCheckBatchAuthorizationPartialFailure() {
    PatchEntityInput input1 = createGlossaryTermInput("urn:li:glossaryTerm:test1", "Term 1");
    PatchEntityInput input2 = createGlossaryTermInput("urn:li:glossaryTerm:test2", "Term 2");

    QueryContext denyContext = TestUtils.getMockDenyContext("urn:li:corpuser:test-user");
    List<PatchEntityInput> inputs = Arrays.asList(input1, input2);

    PatchResolverUtils.checkBatchAuthorization(inputs, denyContext);
  }

  @Test
  public void testCheckBatchAuthorizationEmptyList() {
    PatchResolverUtils.checkBatchAuthorization(new ArrayList<>(), _context);
  }

  // ==================== Helper Methods ====================

  private PatchEntityInput createGlossaryTermInput(String urn, String name) {
    PatchEntityInput input = new PatchEntityInput();
    input.setUrn(urn);
    input.setEntityType("glossaryTerm");
    input.setAspectName("glossaryTermInfo");
    input.setPatch(
        Arrays.asList(
            createPatchOperation(PatchOperationType.ADD, "/name", "\"" + name + "\""),
            createPatchOperation(PatchOperationType.ADD, "/definition", "\"Test definition\"")));
    return input;
  }

  private PatchOperationInput createPatchOperation(
      PatchOperationType op, String path, String value) {
    PatchOperationInput operation = new PatchOperationInput();
    operation.setOp(op);
    operation.setPath(path);
    operation.setValue(value);
    return operation;
  }

  private ArrayPrimaryKeyInput createArrayPrimaryKeyInput(String arrayField, List<String> keys) {
    ArrayPrimaryKeyInput input = new ArrayPrimaryKeyInput();
    input.setArrayField(arrayField);
    input.setKeys(keys);
    return input;
  }

  private void setupEntityRegistryMocks() {
    EntitySpec entitySpec = mock(EntitySpec.class);
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(_entityRegistry.getEntitySpec("glossaryTerm")).thenReturn(entitySpec);
    when(entitySpec.getAspectSpec("glossaryTermInfo")).thenReturn(aspectSpec);
  }
}
