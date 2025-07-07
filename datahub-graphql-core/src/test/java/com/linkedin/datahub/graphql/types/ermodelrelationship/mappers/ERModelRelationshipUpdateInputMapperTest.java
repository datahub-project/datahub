package com.linkedin.datahub.graphql.types.ermodelrelationship.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipEditablePropertiesUpdate;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipPropertiesInput;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipUpdateInput;
import com.linkedin.datahub.graphql.generated.RelationshipFieldMappingInput;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ERModelRelationshipUpdateInputMapperTest {

  private static final Urn TEST_ACTOR_URN = Urn.createFromTuple("corpuser", "testActor");

  @Test
  public void testMapProperties() throws URISyntaxException {
    ERModelRelationshipPropertiesInput propertiesInput = new ERModelRelationshipPropertiesInput();
    propertiesInput.setName("Test Relationship");
    propertiesInput.setSource("urn:li:dataset:(testPlatform,testSource,PROD)");
    propertiesInput.setDestination("urn:li:dataset:(testPlatform,testDestination,PROD)");
    propertiesInput.setRelationshipFieldmappings(
        List.of(
            new RelationshipFieldMappingInput("sourceField1", "destinationField1"),
            new RelationshipFieldMappingInput("sourceField2", "destinationField2")));

    ERModelRelationshipUpdateInput updateInput = new ERModelRelationshipUpdateInput();
    updateInput.setProperties(propertiesInput);

    Collection<MetadataChangeProposal> proposals =
        ERModelRelationshipUpdateInputMapper.map(null, updateInput, TEST_ACTOR_URN);

    Assert.assertEquals(proposals.size(), 1);
    MetadataChangeProposal proposal = proposals.iterator().next();
    Assert.assertEquals(proposal.getAspectName(), "erModelRelationshipProperties");
    Assert.assertNotNull(proposal.getAspect());
  }

  @Test
  public void testMapEditableProperties() {
    ERModelRelationshipEditablePropertiesUpdate editablePropertiesInput =
        new ERModelRelationshipEditablePropertiesUpdate();
    editablePropertiesInput.setName("Editable Name");
    editablePropertiesInput.setDescription("Editable Description");

    ERModelRelationshipUpdateInput updateInput = new ERModelRelationshipUpdateInput();
    updateInput.setEditableProperties(editablePropertiesInput);

    Collection<MetadataChangeProposal> proposals =
        ERModelRelationshipUpdateInputMapper.map(null, updateInput, TEST_ACTOR_URN);

    Assert.assertEquals(proposals.size(), 1);
    MetadataChangeProposal proposal = proposals.iterator().next();
    Assert.assertEquals(proposal.getAspectName(), "editableERModelRelationshipProperties");
    Assert.assertNotNull(proposal.getAspect());
  }

  @Test
  public void testCreateERModelRelationProperties() throws URISyntaxException {
    ERModelRelationshipPropertiesInput propertiesInput = new ERModelRelationshipPropertiesInput();
    propertiesInput.setName("Test Relationship");
    propertiesInput.setSource("urn:li:dataset:(testPlatform,testSource,PROD)");
    propertiesInput.setDestination("urn:li:dataset:(testPlatform,testDestination,PROD)");
    propertiesInput.setRelationshipFieldmappings(
        List.of(
            new RelationshipFieldMappingInput("sourceField1", "destinationField1"),
            new RelationshipFieldMappingInput("sourceField2", "destinationField2")));

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(TEST_ACTOR_URN);
    auditStamp.setTime(System.currentTimeMillis());

    ERModelRelationshipUpdateInputMapper mapper = new ERModelRelationshipUpdateInputMapper();
    com.linkedin.ermodelrelation.ERModelRelationshipProperties properties =
        mapper.createERModelRelationProperties(propertiesInput, auditStamp);

    Assert.assertEquals(properties.getName(), "Test Relationship");
    Assert.assertEquals(
        properties.getSource(),
        DatasetUrn.createFromString("urn:li:dataset:(testPlatform,testSource,PROD)"));
    Assert.assertEquals(
        properties.getDestination(),
        DatasetUrn.createFromString("urn:li:dataset:(testPlatform,testDestination,PROD)"));
    Assert.assertNotNull(properties.getRelationshipFieldMappings());
  }

  @Test
  public void testCardinalitySettings() {
    List<RelationshipFieldMappingInput> fieldMappings =
        List.of(
            new RelationshipFieldMappingInput("sourceField1", "destinationField1"),
            new RelationshipFieldMappingInput("sourceField2", "destinationField2"));

    ERModelRelationshipUpdateInputMapper mapper = new ERModelRelationshipUpdateInputMapper();
    com.linkedin.ermodelrelation.ERModelRelationshipCardinality cardinality =
        mapper.ermodelrelationCardinalitySettings(fieldMappings, null);

    Assert.assertEquals(
        cardinality, com.linkedin.ermodelrelation.ERModelRelationshipCardinality.N_N);
  }

  @Test
  public void testFieldMappingSettings() {
    List<RelationshipFieldMappingInput> fieldMappings =
        List.of(
            new RelationshipFieldMappingInput("sourceField1", "destinationField1"),
            new RelationshipFieldMappingInput("sourceField2", "destinationField2"));

    ERModelRelationshipUpdateInputMapper mapper = new ERModelRelationshipUpdateInputMapper();
    com.linkedin.ermodelrelation.RelationshipFieldMappingArray mappingArray =
        mapper.ermodelrelationFieldMappingSettings(fieldMappings);

    Assert.assertEquals(mappingArray.size(), 2);
    Assert.assertEquals(mappingArray.get(0).getSourceField(), "sourceField1");
    Assert.assertEquals(mappingArray.get(0).getDestinationField(), "destinationField1");
  }
}
