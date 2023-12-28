package com.linkedin.datahub.graphql.types.dataplatforminstance;

import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataplatforminstance.DataPlatformInstanceProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataPlatformInstanceKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataPlatformInstanceTest {

  private static final Urn TEST_ACTOR_URN =
      Urn.createFromTuple(Constants.CORP_USER_ENTITY_NAME, "test");

  private static final String TEST_DATAPLATFORMINSTANCE_1_URN =
      "urn:li:dataPlatformInstance:(urn:li:dataPlatform:P,I1)";

  private static final DataPlatformInstanceKey TEST_DATAPLATFORMINSTANCE_1_KEY =
      new DataPlatformInstanceKey()
          .setPlatform(Urn.createFromTuple(Constants.DATA_PLATFORM_ENTITY_NAME, "P"))
          .setInstance("I1");

  private static final DataPlatformInstanceProperties TEST_DATAPLATFORMINSTANCE_1_PROPERTIES =
      new DataPlatformInstanceProperties()
          .setDescription("test description")
          .setName("Test Data Platform Instance");

  private static final Deprecation TEST_DATAPLATFORMINSTANCE_1_DEPRECATION =
      new Deprecation().setDeprecated(true).setActor(TEST_ACTOR_URN).setNote("legacy");

  private static final Ownership TEST_DATAPLATFORMINSTANCE_1_OWNERSHIP =
      new Ownership()
          .setOwners(
              new OwnerArray(
                  ImmutableList.of(
                      new Owner().setType(OwnershipType.DATAOWNER).setOwner(TEST_ACTOR_URN))));

  private static final InstitutionalMemory TEST_DATAPLATFORMINSTANCE_1_INSTITUTIONAL_MEMORY =
      new InstitutionalMemory()
          .setElements(
              new InstitutionalMemoryMetadataArray(
                  ImmutableList.of(
                      new InstitutionalMemoryMetadata()
                          .setUrl(new Url("https://www.test.com"))
                          .setDescription("test description")
                          .setCreateStamp(new AuditStamp().setTime(0L).setActor(TEST_ACTOR_URN)))));

  private static final GlobalTags TEST_DATAPLATFORMINSTANCE_1_TAGS =
      new GlobalTags()
          .setTags(
              new TagAssociationArray(
                  ImmutableList.of(new TagAssociation().setTag(new TagUrn("test")))));

  private static final Status TEST_DATAPLATFORMINSTANCE_1_STATUS = new Status().setRemoved(false);

  private static final String TEST_DATAPLATFORMINSTANCE_2_URN =
      "urn:li:dataPlatformInstance:(urn:li:dataPlatform:P,I2)";

  @Test
  public void testBatchLoad() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Urn dataPlatformInstance1Urn = Urn.createFromString(TEST_DATAPLATFORMINSTANCE_1_URN);
    Urn dataPlatformInstance2Urn = Urn.createFromString(TEST_DATAPLATFORMINSTANCE_2_URN);

    Map<String, EnvelopedAspect> dataPlatformInstance1Aspects = new HashMap<>();
    dataPlatformInstance1Aspects.put(
        Constants.DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATAPLATFORMINSTANCE_1_KEY.data())));
    dataPlatformInstance1Aspects.put(
        Constants.DATA_PLATFORM_INSTANCE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATAPLATFORMINSTANCE_1_PROPERTIES.data())));
    dataPlatformInstance1Aspects.put(
        Constants.DEPRECATION_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATAPLATFORMINSTANCE_1_DEPRECATION.data())));
    dataPlatformInstance1Aspects.put(
        Constants.OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATAPLATFORMINSTANCE_1_OWNERSHIP.data())));
    dataPlatformInstance1Aspects.put(
        Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(TEST_DATAPLATFORMINSTANCE_1_INSTITUTIONAL_MEMORY.data())));
    dataPlatformInstance1Aspects.put(
        Constants.GLOBAL_TAGS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATAPLATFORMINSTANCE_1_TAGS.data())));
    dataPlatformInstance1Aspects.put(
        Constants.STATUS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATAPLATFORMINSTANCE_1_STATUS.data())));
    Mockito.when(
            client.batchGetV2(
                Mockito.eq(Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME),
                Mockito.eq(
                    new HashSet<>(
                        ImmutableSet.of(dataPlatformInstance1Urn, dataPlatformInstance2Urn))),
                Mockito.eq(DataPlatformInstanceType.ASPECTS_TO_FETCH),
                Mockito.any(Authentication.class)))
        .thenReturn(
            ImmutableMap.of(
                dataPlatformInstance1Urn,
                new EntityResponse()
                    .setEntityName(Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME)
                    .setUrn(dataPlatformInstance1Urn)
                    .setAspects(new EnvelopedAspectMap(dataPlatformInstance1Aspects))));

    DataPlatformInstanceType type = new DataPlatformInstanceType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    List<DataFetcherResult<DataPlatformInstance>> result =
        type.batchLoad(
            ImmutableList.of(TEST_DATAPLATFORMINSTANCE_1_URN, TEST_DATAPLATFORMINSTANCE_2_URN),
            mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            Mockito.eq(Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(dataPlatformInstance1Urn, dataPlatformInstance2Urn)),
            Mockito.eq(DataPlatformInstanceType.ASPECTS_TO_FETCH),
            Mockito.any(Authentication.class));

    assertEquals(result.size(), 2);

    DataPlatformInstance dataPlatformInstance1 = result.get(0).getData();
    assertEquals(dataPlatformInstance1.getUrn(), TEST_DATAPLATFORMINSTANCE_1_URN);
    assertEquals(dataPlatformInstance1.getType(), EntityType.DATA_PLATFORM_INSTANCE);
    assertEquals(
        dataPlatformInstance1.getProperties().getDescription(),
        TEST_DATAPLATFORMINSTANCE_1_PROPERTIES.getDescription());
    assertEquals(
        dataPlatformInstance1.getProperties().getName(),
        TEST_DATAPLATFORMINSTANCE_1_PROPERTIES.getName());
    assertEquals(
        dataPlatformInstance1.getDeprecation().getDeprecated(),
        TEST_DATAPLATFORMINSTANCE_1_DEPRECATION.isDeprecated().booleanValue());
    assertEquals(
        dataPlatformInstance1.getDeprecation().getNote(),
        TEST_DATAPLATFORMINSTANCE_1_DEPRECATION.getNote());
    assertEquals(
        dataPlatformInstance1.getDeprecation().getActor(),
        TEST_DATAPLATFORMINSTANCE_1_DEPRECATION.getActor().toString());
    assertEquals(dataPlatformInstance1.getOwnership().getOwners().size(), 1);
    assertEquals(dataPlatformInstance1.getInstitutionalMemory().getElements().size(), 1);
    assertEquals(
        dataPlatformInstance1.getTags().getTags().get(0).getTag().getUrn(),
        TEST_DATAPLATFORMINSTANCE_1_TAGS.getTags().get(0).getTag().toString());
    assertEquals(
        dataPlatformInstance1.getStatus().getRemoved(),
        TEST_DATAPLATFORMINSTANCE_1_STATUS.isRemoved().booleanValue());

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(
            Mockito.anyString(),
            Mockito.anySet(),
            Mockito.anySet(),
            Mockito.any(Authentication.class));
    com.linkedin.datahub.graphql.types.dataplatforminstance.DataPlatformInstanceType type =
        new com.linkedin.datahub.graphql.types.dataplatforminstance.DataPlatformInstanceType(
            mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () ->
            type.batchLoad(
                ImmutableList.of(TEST_DATAPLATFORMINSTANCE_1_URN, TEST_DATAPLATFORMINSTANCE_2_URN),
                context));
  }
}
