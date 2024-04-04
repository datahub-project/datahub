package com.datahub.notification.provider;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityNameProviderTest {
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)");
  private static final Urn TEST_DATA_PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:hive");

  private EntityClient entityClient;
  private Authentication systemAuthentication;
  private EntityNameProvider entityNameProvider;

  @BeforeMethod
  public void setUp() {
    entityClient = Mockito.mock(EntityClient.class);
    systemAuthentication = Mockito.mock(Authentication.class);
    entityNameProvider = new EntityNameProvider(entityClient, systemAuthentication);
  }

  @Test
  public void testGetNameForDifferentEntityTypes() {
    try {
      Mockito.when(
              entityClient.getV2(
                  Mockito.eq(TEST_DATASET_URN.getEntityType()),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(ImmutableSet.of(DATASET_PROPERTIES_ASPECT_NAME)),
                  Mockito.any(Authentication.class)))
          .thenReturn(getMockDatasetNameResponse("Expected Dataset Name"));
      String name = entityNameProvider.getName(TEST_DATASET_URN);
      Assert.assertEquals(name, "Expected Dataset Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetPlatformNameExists() {
    try {
      Mockito.when(
              entityClient.getV2(
                  Mockito.eq(DATASET_ENTITY_NAME),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(ImmutableSet.of(DATA_PLATFORM_INSTANCE_ASPECT_NAME)),
                  Mockito.any(Authentication.class)))
          .thenReturn(getMockDataPlatformInstanceResponse(TEST_DATA_PLATFORM_URN));
      Mockito.when(
              entityClient.getV2(
                  Mockito.eq(DATA_PLATFORM_ENTITY_NAME),
                  Mockito.eq(TEST_DATA_PLATFORM_URN),
                  Mockito.eq(ImmutableSet.of(DATA_PLATFORM_INFO_ASPECT_NAME)),
                  Mockito.any(Authentication.class)))
          .thenReturn(getMockDataPlatformInfoResponse("Expected Platform Name"));
      String platformName = entityNameProvider.getPlatformName(TEST_DATASET_URN);
      Assert.assertEquals(platformName, "Expected Platform Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetPlatformNameNull() {
    try {
      Mockito.when(
              entityClient.getV2(
                  Mockito.eq(DATASET_ENTITY_NAME),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(ImmutableSet.of(DATASET_PROPERTIES_ASPECT_NAME)),
                  Mockito.any(Authentication.class)))
          .thenReturn(null);
      Assert.assertNull(entityNameProvider.getPlatformName(TEST_DATASET_URN));
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetEntityTypeNameSubTypes() {
    try {
      Mockito.when(
              entityClient.getV2(
                  Mockito.eq(TEST_DATASET_URN.getEntityType()),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(ImmutableSet.of(SUB_TYPES_ASPECT_NAME)),
                  Mockito.any(Authentication.class)))
          .thenReturn(getMockSubTypesResponse("Expected Type Name"));

      String entityTypeName = entityNameProvider.getTypeName(TEST_DATASET_URN);
      Assert.assertNotNull(entityTypeName);
      Assert.assertEquals(entityTypeName, "Expected Type Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetEntityTypeNameSupportedEntityType() {
    try {
      Mockito.when(
              entityClient.getV2(
                  Mockito.eq(TEST_DATASET_URN.getEntityType()),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(ImmutableSet.of(SUB_TYPES_ASPECT_NAME)),
                  Mockito.any(Authentication.class)))
          .thenReturn(null);

      String entityTypeName = entityNameProvider.getTypeName(TEST_DATASET_URN);
      Assert.assertNotNull(entityTypeName);
      Assert.assertEquals(entityTypeName, "Dataset");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetEntityTypeNameUnsupportedEntityType() {
    Urn otherTypeUrn = UrnUtils.getUrn("urn:li:someType:test");
    try {
      Mockito.when(
              entityClient.getV2(
                  Mockito.eq(otherTypeUrn.getEntityType()),
                  Mockito.eq(otherTypeUrn),
                  Mockito.eq(ImmutableSet.of(SUB_TYPES_ASPECT_NAME)),
                  Mockito.any(Authentication.class)))
          .thenReturn(null);

      String entityTypeName = entityNameProvider.getTypeName(otherTypeUrn);
      Assert.assertNotNull(entityTypeName);
      Assert.assertEquals(entityTypeName, "someType");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  private EntityResponse getMockDatasetNameResponse(String name) {
    return new EntityResponse()
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(TEST_DATASET_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    DATASET_PROPERTIES_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(DATASET_PROPERTIES_ASPECT_NAME)
                        .setValue(
                            new Aspect(
                                (new com.linkedin.dataset.DatasetProperties()
                                    .setName(name)
                                    .data()))))));
  }

  private EntityResponse getMockDataPlatformInstanceResponse(Urn urn) {
    return new EntityResponse()
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(urn)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                        .setValue(
                            new Aspect((new DataPlatformInstance().setPlatform(urn).data()))))));
  }

  private EntityResponse getMockDataPlatformInfoResponse(String name) {
    return new EntityResponse()
        .setEntityName(Constants.DATA_PLATFORM_ENTITY_NAME)
        .setUrn(TEST_DATA_PLATFORM_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    DATA_PLATFORM_INFO_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(DATA_PLATFORM_INFO_ASPECT_NAME)
                        .setValue(
                            new Aspect(
                                (new com.linkedin.dataset.DatasetProperties()
                                    .setName(name)
                                    .data()))))));
  }

  private EntityResponse getMockSubTypesResponse(String name) {
    return new EntityResponse()
        .setEntityName(DATASET_ENTITY_NAME)
        .setUrn(TEST_DATASET_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    SUB_TYPES_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(SUB_TYPES_ASPECT_NAME)
                        .setValue(
                            new Aspect(
                                (new SubTypes()
                                    .setTypeNames(new StringArray(ImmutableList.of(name)))
                                    .data()))))));
  }
}
