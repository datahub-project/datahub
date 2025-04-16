package com.linkedin.metadata.entity.validation;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class ValidationApiUtilsTest {
  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final EntityRegistry entityRegistry = opContext.getEntityRegistry();
  private final Urn validUrn =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mongodb,adoption.pet_profiles,PROD)");

  @Test
  public void testValidateOrThrow_ValidRecord() {
    // No validation exception should be thrown
    ValidationApiUtils.validateOrThrow(new DatasetProperties());
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateOrThrow_InvalidRecord() {
    RecordTemplate recordTemplate = new DatasetProperties();
    // Set up the invalid record
    recordTemplate.data().put("INVALID", "foobar");

    ValidationApiUtils.validateOrThrow(recordTemplate);
  }

  @Test
  public void testValidateTrimOrThrow_InvalidRecord() {
    RecordTemplate recordTemplate = new DatasetProperties();
    // Set up the invalid record
    recordTemplate.data().put("INVALID", "foobar");

    ValidationApiUtils.validateTrimOrThrow(recordTemplate);
    assertFalse(recordTemplate.data().containsKey("INVALID"));
  }

  @Test
  public void testValidateUrn_ValidUrn() {
    Urn result = ValidationApiUtils.validateUrn(entityRegistry, validUrn);
    assertEquals(result, validUrn);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateUrn_NullUrn() {
    ValidationApiUtils.validateUrn(entityRegistry, null);
  }

  @Test
  public void testValidateEntity_ValidEntityType() {
    EntitySpec result = ValidationApiUtils.validateEntity(entityRegistry, "dataset");
    assertEquals(result, entityRegistry.getEntitySpec("dataset"));
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateEntity_InvalidEntityType() {
    ValidationApiUtils.validateEntity(entityRegistry, "invalidEntity");
  }

  @Test
  public void testValidateAspect_ValidAspect() {
    EntitySpec entitySpec = entityRegistry.getEntitySpec("dataset");
    AspectSpec result = ValidationApiUtils.validateAspect(entitySpec, "datasetProperties");
    assertEquals(result, entitySpec.getAspectSpec("datasetProperties"));
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testValidateAspect_InvalidAspect() {
    EntitySpec entitySpec = entityRegistry.getEntitySpec("dataset");
    ValidationApiUtils.validateAspect(entitySpec, "invalidAspect");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testValidateAspect_NullAspectName() {
    EntitySpec entitySpec = entityRegistry.getEntitySpec("dataset");
    ValidationApiUtils.validateAspect(entitySpec, null);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testValidateAspect_EmptyAspectName() {
    EntitySpec entitySpec = entityRegistry.getEntitySpec("dataset");
    ValidationApiUtils.validateAspect(entitySpec, "");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testValidateMCP_NullMCP() {
    ValidationApiUtils.validateMCP(entityRegistry, null);
  }

  @Test
  public void testValidateMCP_WithEntityUrn() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(validUrn);
    mcp.setEntityType("dataset");
    mcp.setAspectName("datasetProperties");

    EntityRegistry mockEntityRegistry = spy(entityRegistry);
    MetadataChangeProposal result = ValidationApiUtils.validateMCP(mockEntityRegistry, mcp);

    verify(mockEntityRegistry, times(3)).getEntitySpec("dataset");
    assertEquals(result, mcp);
  }

  @Test
  public void testValidateMCP_WithoutEntityUrn() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType("dataset");
    mcp.setAspectName("datasetProperties");
    mcp.setEntityKeyAspect(
        GenericRecordUtils.serializeAspect(
            new DatasetKey()
                .setPlatform(UrnUtils.getUrn("urn:li:dataPlatform:mongodb"))
                .setName("adoption.pet_profiles")
                .setOrigin(FabricType.PROD)));

    EntityRegistry mockEntityRegistry = spy(entityRegistry);
    MetadataChangeProposal result = ValidationApiUtils.validateMCP(mockEntityRegistry, mcp);

    verify(mockEntityRegistry, times(3)).getEntitySpec("dataset");
    assertEquals(result.getEntityUrn(), validUrn);
  }

  @Test
  public void testValidateMCP_EntityTypesCaseInsensitiveMatch() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(validUrn);
    mcp.setEntityType("DATASET");
    mcp.setAspectName("datasetProperties");

    MetadataChangeProposal result = ValidationApiUtils.validateMCP(entityRegistry, mcp);

    // Verify the MCP's entity type was corrected
    assertEquals(result.getEntityType(), "dataset");
  }

  @Test
  public void testValidateMCP_SuccessfulValidation() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(validUrn);
    mcp.setEntityType("dataset");
    mcp.setAspectName("datasetProperties");

    MetadataChangeProposal result = ValidationApiUtils.validateMCP(entityRegistry, mcp);

    assertEquals(result, mcp);
  }
}
