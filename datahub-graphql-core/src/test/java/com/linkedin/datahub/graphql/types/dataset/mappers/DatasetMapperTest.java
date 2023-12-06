package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatasetMapperTest {

  private static final Urn TEST_DATASET_URN =
      Urn.createFromTuple(Constants.DATASET_ENTITY_NAME, "test");
  private static final Urn TEST_CREATED_ACTOR_URN =
      Urn.createFromTuple(Constants.CORP_USER_ENTITY_NAME, "created");
  private static final Urn TEST_LAST_MODIFIED_ACTOR_URN =
      Urn.createFromTuple(Constants.CORP_USER_ENTITY_NAME, "lastmodified");

  @Test
  public void testDatasetPropertiesMapperWithCreatedAndLastModified() {
    final com.linkedin.dataset.DatasetProperties input =
        new com.linkedin.dataset.DatasetProperties();
    input.setName("Test");
    input.setQualifiedName("Test QualifiedName");

    final TimeStamp createdTimestamp = new TimeStamp();
    createdTimestamp.setActor(TEST_CREATED_ACTOR_URN);
    createdTimestamp.setTime(10L);
    input.setCreated(createdTimestamp);

    final TimeStamp lastModifiedTimestamp = new TimeStamp();
    lastModifiedTimestamp.setActor(TEST_LAST_MODIFIED_ACTOR_URN);
    lastModifiedTimestamp.setTime(20L);
    input.setLastModified(lastModifiedTimestamp);

    final Map<String, com.linkedin.entity.EnvelopedAspect> dataSetPropertiesAspects =
        new HashMap<>();
    dataSetPropertiesAspects.put(
        Constants.DATASET_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(dataSetPropertiesAspects));
    final Dataset actual = DatasetMapper.map(response);

    final Dataset expected = new Dataset();
    expected.setUrn(TEST_DATASET_URN.toString());
    final DatasetProperties expectedDatasetProperties = new DatasetProperties();
    expectedDatasetProperties.setName("Test");
    expectedDatasetProperties.setQualifiedName("Test QualifiedName");
    expectedDatasetProperties.setLastModifiedActor(TEST_LAST_MODIFIED_ACTOR_URN.toString());
    expectedDatasetProperties.setCreatedActor(TEST_CREATED_ACTOR_URN.toString());
    expectedDatasetProperties.setLastModified(
        new AuditStamp(20L, TEST_LAST_MODIFIED_ACTOR_URN.toString()));
    expectedDatasetProperties.setCreated(10L);
    expected.setProperties(expectedDatasetProperties);

    Assert.assertEquals(actual.getUrn(), expected.getUrn());
    Assert.assertEquals(actual.getProperties().getName(), expected.getProperties().getName());
    Assert.assertEquals(
        actual.getProperties().getQualifiedName(), expected.getProperties().getQualifiedName());

    Assert.assertEquals(
        actual.getProperties().getLastModified().getTime(),
        expected.getProperties().getLastModified().getTime());
    Assert.assertEquals(
        actual.getProperties().getLastModified().getActor(),
        expected.getProperties().getLastModified().getActor());
    Assert.assertEquals(actual.getProperties().getCreated(), expected.getProperties().getCreated());

    Assert.assertEquals(
        actual.getProperties().getLastModifiedActor(),
        expected.getProperties().getLastModifiedActor());
    Assert.assertEquals(
        actual.getProperties().getCreatedActor(), expected.getProperties().getCreatedActor());
  }

  @Test
  public void testDatasetPropertiesMapperWithoutCreatedAndLastModified() {
    final com.linkedin.dataset.DatasetProperties input =
        new com.linkedin.dataset.DatasetProperties();
    input.setName("Test");

    final Map<String, com.linkedin.entity.EnvelopedAspect> dataSetPropertiesAspects =
        new HashMap<>();
    dataSetPropertiesAspects.put(
        Constants.DATASET_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(dataSetPropertiesAspects));
    final Dataset actual = DatasetMapper.map(response);

    final Dataset expected = new Dataset();
    expected.setUrn(TEST_DATASET_URN.toString());
    final DatasetProperties expectedDatasetProperties = new DatasetProperties();
    expectedDatasetProperties.setName("Test");
    expectedDatasetProperties.setLastModifiedActor(null);
    expectedDatasetProperties.setCreatedActor(null);
    expectedDatasetProperties.setLastModified(new AuditStamp(0L, null));
    expectedDatasetProperties.setCreated(null);
    expected.setProperties(expectedDatasetProperties);

    Assert.assertEquals(actual.getUrn(), expected.getUrn());
    Assert.assertEquals(actual.getProperties().getName(), expected.getProperties().getName());

    Assert.assertEquals(
        actual.getProperties().getLastModified().getTime(),
        expected.getProperties().getLastModified().getTime());
    Assert.assertEquals(
        actual.getProperties().getLastModified().getActor(),
        expected.getProperties().getLastModified().getActor());
    Assert.assertEquals(actual.getProperties().getCreated(), expected.getProperties().getCreated());

    Assert.assertEquals(
        actual.getProperties().getLastModifiedActor(),
        expected.getProperties().getLastModifiedActor());
    Assert.assertEquals(
        actual.getProperties().getCreatedActor(), expected.getProperties().getCreatedActor());
  }

  @Test
  public void testDatasetPropertiesMapperWithoutTimestampActors() {
    final com.linkedin.dataset.DatasetProperties input =
        new com.linkedin.dataset.DatasetProperties();
    input.setName("Test");

    TimeStamp createdTimestamp = new TimeStamp();
    createdTimestamp.setTime(10L);
    input.setCreated(createdTimestamp);

    TimeStamp lastModifiedTimestamp = new TimeStamp();
    lastModifiedTimestamp.setTime(20L);
    input.setLastModified(lastModifiedTimestamp);

    final Map<String, com.linkedin.entity.EnvelopedAspect> dataSetPropertiesAspects =
        new HashMap<>();
    dataSetPropertiesAspects.put(
        Constants.DATASET_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(dataSetPropertiesAspects));
    final Dataset actual = DatasetMapper.map(response);

    final Dataset expected = new Dataset();
    expected.setUrn(TEST_DATASET_URN.toString());
    final DatasetProperties expectedDatasetProperties = new DatasetProperties();
    expectedDatasetProperties.setName("Test");
    expectedDatasetProperties.setLastModifiedActor(null);
    expectedDatasetProperties.setCreatedActor(null);
    expectedDatasetProperties.setLastModified(new AuditStamp(20L, null));
    expectedDatasetProperties.setCreated(10L);
    expected.setProperties(expectedDatasetProperties);

    Assert.assertEquals(actual.getUrn(), expected.getUrn());
    Assert.assertEquals(actual.getProperties().getName(), expected.getProperties().getName());

    Assert.assertEquals(
        actual.getProperties().getLastModified().getTime(),
        expected.getProperties().getLastModified().getTime());
    Assert.assertEquals(
        actual.getProperties().getLastModified().getActor(),
        expected.getProperties().getLastModified().getActor());
    Assert.assertEquals(actual.getProperties().getCreated(), expected.getProperties().getCreated());

    Assert.assertEquals(
        actual.getProperties().getLastModifiedActor(),
        expected.getProperties().getLastModifiedActor());
    Assert.assertEquals(
        actual.getProperties().getCreatedActor(), expected.getProperties().getCreatedActor());
  }
}
