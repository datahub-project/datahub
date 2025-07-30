package com.linkedin.datahub.graphql.types.ermodelrelationship.mappers;

import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ERModelRelationship;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.ermodelrelation.ERModelRelationshipCardinality;
import com.linkedin.ermodelrelation.ERModelRelationshipProperties;
import com.linkedin.ermodelrelation.EditableERModelRelationshipProperties;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ERModelRelationshipKey;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ERModelRelationMapperTest {

  private static final Urn TEST_ER_MODEL_RELATIONSHIP_URN =
      Urn.createFromTuple(Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME, "relation1");

  @Test
  public void testMapERModelRelationshipKey() throws URISyntaxException {
    ERModelRelationshipKey inputKey = new ERModelRelationshipKey();
    inputKey.setId("relation1");

    final Map<String, EnvelopedAspect> keyAspect = new HashMap<>();
    keyAspect.put(
        Constants.ER_MODEL_RELATIONSHIP_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inputKey.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME)
            .setUrn(TEST_ER_MODEL_RELATIONSHIP_URN)
            .setAspects(new EnvelopedAspectMap(keyAspect));

    final ERModelRelationship actual = ERModelRelationMapper.map(null, response);

    Assert.assertEquals(actual.getUrn(), TEST_ER_MODEL_RELATIONSHIP_URN.toString());
    Assert.assertEquals(actual.getId(), "relation1");
  }

  @Test
  public void testMapEditableProperties() throws URISyntaxException {
    EditableERModelRelationshipProperties inputEditableProperties =
        new EditableERModelRelationshipProperties();
    inputEditableProperties.setName("Editable Name");
    inputEditableProperties.setDescription("Editable Description");

    final Map<String, EnvelopedAspect> editableAspect = new HashMap<>();
    editableAspect.put(
        Constants.EDITABLE_ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inputEditableProperties.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME)
            .setUrn(TEST_ER_MODEL_RELATIONSHIP_URN)
            .setAspects(new EnvelopedAspectMap(editableAspect));

    final ERModelRelationship actual = ERModelRelationMapper.map(null, response);

    Assert.assertEquals(actual.getEditableProperties().getName(), "Editable Name");
    Assert.assertEquals(actual.getEditableProperties().getDescription(), "Editable Description");
  }

  @Test
  public void testMapProperties() throws URISyntaxException {
    ERModelRelationshipProperties inputProperties = new ERModelRelationshipProperties();
    inputProperties.setName("Relationship Name");
    inputProperties.setCardinality(ERModelRelationshipCardinality.ONE_ONE);
    inputProperties.setSource(Urn.createFromTuple("dataset", "sourceDataset"));
    inputProperties.setDestination(Urn.createFromTuple("dataset", "destinationDataset"));
    inputProperties.setCreated(
        new com.linkedin.common.AuditStamp()
            .setActor(Urn.createFromTuple("corpuser", "testActor"))
            .setTime(System.currentTimeMillis()));

    final Map<String, EnvelopedAspect> propertiesAspect = new HashMap<>();
    propertiesAspect.put(
        Constants.ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inputProperties.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME)
            .setUrn(TEST_ER_MODEL_RELATIONSHIP_URN)
            .setAspects(new EnvelopedAspectMap(propertiesAspect));

    final ERModelRelationship actual = ERModelRelationMapper.map(null, response);

    Assert.assertEquals(actual.getProperties().getName(), "Relationship Name");
    Assert.assertEquals(actual.getProperties().getCardinality().name(), "ONE_ONE");
    Assert.assertEquals(
        actual.getProperties().getSource().getUrn(), "urn:li:dataset:sourceDataset");
    Assert.assertEquals(
        actual.getProperties().getDestination().getUrn(), "urn:li:dataset:destinationDataset");
  }

  @Test
  public void testMapOwnership() throws URISyntaxException {
    Ownership inputOwnership = new Ownership();
    inputOwnership.setOwners(new OwnerArray());

    final Map<String, EnvelopedAspect> ownershipAspect = new HashMap<>();
    ownershipAspect.put(
        Constants.OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inputOwnership.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME)
            .setUrn(TEST_ER_MODEL_RELATIONSHIP_URN)
            .setAspects(new EnvelopedAspectMap(ownershipAspect));

    final ERModelRelationship actual = ERModelRelationMapper.map(null, response);

    Assert.assertNotNull(actual.getOwnership());
  }

  @Test
  public void testMapStatus() throws URISyntaxException {
    Status inputStatus = new Status();
    inputStatus.setRemoved(false);

    final Map<String, EnvelopedAspect> statusAspect = new HashMap<>();
    statusAspect.put(
        Constants.STATUS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(inputStatus.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.ER_MODEL_RELATIONSHIP_ENTITY_NAME)
            .setUrn(TEST_ER_MODEL_RELATIONSHIP_URN)
            .setAspects(new EnvelopedAspectMap(statusAspect));

    final ERModelRelationship actual = ERModelRelationMapper.map(null, response);

    Assert.assertNotNull(actual.getStatus());
    Assert.assertFalse(actual.getStatus().getRemoved());
  }
}
