package com.datahub.notification.provider;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_TYPE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_TYPE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.ownership.OwnershipTypeInfo;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityNameProviderTest {
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)");
  private static final Urn TEST_DATA_PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:hive");
  private static final Urn TEST_OWNERSHIP_TYPE_URN = UrnUtils.getUrn("urn:li:ownershipType:test");
  private static final Urn TEST_STRUCTURED_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:test");
  private static final Urn TEST_GLOSSARY_TERM_URN = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  private static final Urn TEST_GLOSSARY_NODE_URN = UrnUtils.getUrn("urn:li:glossaryNode:test");

  private SystemEntityClient entityClient;
  private Authentication systemAuthentication;
  private EntityNameProvider entityNameProvider;

  @BeforeMethod
  public void setUp() {
    entityClient = mock(SystemEntityClient.class);
    systemAuthentication = mock(Authentication.class);
    entityNameProvider = new EntityNameProvider(entityClient);
  }

  @Test
  public void testGetNameForDifferentEntityTypes() {
    try {
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(TEST_DATASET_URN.getEntityType()),
                  Mockito.eq(Set.of(TEST_DATASET_URN)),
                  Mockito.eq(ImmutableSet.of(DATASET_PROPERTIES_ASPECT_NAME))))
          .thenReturn(
              Map.of(TEST_DATASET_URN, getMockDatasetNameResponse("Expected Dataset Name")));
      String name = entityNameProvider.getName(mock(OperationContext.class), TEST_DATASET_URN);
      Assert.assertEquals(name, "Expected Dataset Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetPlatformNameExists() {
    try {
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(DATASET_ENTITY_NAME),
                  Mockito.eq(Set.of(TEST_DATASET_URN)),
                  Mockito.eq(ImmutableSet.of(DATA_PLATFORM_INSTANCE_ASPECT_NAME))))
          .thenReturn(
              Map.of(
                  TEST_DATASET_URN, getMockDataPlatformInstanceResponse(TEST_DATA_PLATFORM_URN)));
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(DATA_PLATFORM_ENTITY_NAME),
                  Mockito.eq(Set.of(TEST_DATA_PLATFORM_URN)),
                  Mockito.eq(ImmutableSet.of(DATA_PLATFORM_INFO_ASPECT_NAME))))
          .thenReturn(
              Map.of(
                  TEST_DATA_PLATFORM_URN,
                  getMockDataPlatformInfoResponse("Expected Platform Name")));
      String platformName =
          entityNameProvider.getPlatformName(mock(OperationContext.class), TEST_DATASET_URN);
      Assert.assertEquals(platformName, "Expected Platform Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetOwnershipTypeNameExists() {
    try {
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(OWNERSHIP_TYPE_ENTITY_NAME),
                  Mockito.eq(Set.of(TEST_OWNERSHIP_TYPE_URN)),
                  Mockito.eq(ImmutableSet.of(OWNERSHIP_TYPE_INFO_ASPECT_NAME))))
          .thenReturn(
              Map.of(
                  TEST_OWNERSHIP_TYPE_URN,
                  getMockOwnershipTypeResponse("Expected Ownership Type Name")));
      String name =
          entityNameProvider.getName(mock(OperationContext.class), TEST_OWNERSHIP_TYPE_URN);
      Assert.assertEquals(name, "Expected Ownership Type Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetStructuredPropertyNameExists() {
    try {
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(STRUCTURED_PROPERTY_ENTITY_NAME),
                  Mockito.eq(Set.of(TEST_STRUCTURED_PROPERTY_URN)),
                  Mockito.eq(ImmutableSet.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))))
          .thenReturn(
              Map.of(
                  TEST_STRUCTURED_PROPERTY_URN,
                  getMockStructuredPropertyResponse("Expected Structured Property Name")));
      String name =
          entityNameProvider.getName(mock(OperationContext.class), TEST_STRUCTURED_PROPERTY_URN);
      Assert.assertEquals(name, "Expected Structured Property Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetGlossaryTermNameExists() {
    try {
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(GLOSSARY_TERM_ENTITY_NAME),
                  Mockito.eq(Set.of(TEST_GLOSSARY_TERM_URN)),
                  Mockito.eq(ImmutableSet.of(GLOSSARY_TERM_INFO_ASPECT_NAME))))
          .thenReturn(
              Map.of(
                  TEST_GLOSSARY_TERM_URN,
                  getMockGlossaryTermResponse("Expected Glossary Term Name")));
      String name =
          entityNameProvider.getName(mock(OperationContext.class), TEST_GLOSSARY_TERM_URN);
      Assert.assertEquals(name, "Expected Glossary Term Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetGlossaryNodeNameExists() {
    try {
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(GLOSSARY_NODE_ENTITY_NAME),
                  Mockito.eq(Set.of(TEST_GLOSSARY_NODE_URN)),
                  Mockito.eq(ImmutableSet.of(GLOSSARY_NODE_INFO_ASPECT_NAME))))
          .thenReturn(
              Map.of(
                  TEST_GLOSSARY_NODE_URN,
                  getMockGlossaryNodeResponse("Expected Glossary Node Name")));
      String name =
          entityNameProvider.getName(mock(OperationContext.class), TEST_GLOSSARY_NODE_URN);
      Assert.assertEquals(name, "Expected Glossary Node Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetPlatformNameNull() {
    try {
      Mockito.when(
              entityClient.getV2(
                  any(OperationContext.class),
                  Mockito.eq(DATASET_ENTITY_NAME),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(ImmutableSet.of(DATASET_PROPERTIES_ASPECT_NAME))))
          .thenReturn(null);
      Assert.assertNull(
          entityNameProvider.getPlatformName(mock(OperationContext.class), TEST_DATASET_URN));
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetEntityTypeNameSubTypes() {
    try {
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(TEST_DATASET_URN.getEntityType()),
                  Mockito.eq(Set.of(TEST_DATASET_URN)),
                  Mockito.eq(ImmutableSet.of(SUB_TYPES_ASPECT_NAME))))
          .thenReturn(Map.of(TEST_DATASET_URN, getMockSubTypesResponse("Expected Type Name")));

      String entityTypeName =
          entityNameProvider.getTypeName(mock(OperationContext.class), TEST_DATASET_URN);
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
                  any(OperationContext.class),
                  Mockito.eq(TEST_DATASET_URN.getEntityType()),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(ImmutableSet.of(SUB_TYPES_ASPECT_NAME))))
          .thenReturn(null);

      String entityTypeName =
          entityNameProvider.getTypeName(mock(OperationContext.class), TEST_DATASET_URN);
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
                  any(OperationContext.class),
                  Mockito.eq(otherTypeUrn.getEntityType()),
                  Mockito.eq(otherTypeUrn),
                  Mockito.eq(ImmutableSet.of(SUB_TYPES_ASPECT_NAME))))
          .thenReturn(null);

      String entityTypeName =
          entityNameProvider.getTypeName(mock(OperationContext.class), otherTypeUrn);
      Assert.assertNotNull(entityTypeName);
      Assert.assertEquals(entityTypeName, "someType");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  @Test
  public void testGetQualifiedName() {
    try {
      Mockito.when(
              entityClient.batchGetV2(
                  any(OperationContext.class),
                  Mockito.eq(TEST_DATASET_URN.getEntityType()),
                  Mockito.eq(Set.of(TEST_DATASET_URN)),
                  Mockito.eq(ImmutableSet.of(DATASET_PROPERTIES_ASPECT_NAME))))
          .thenReturn(
              Map.of(
                  TEST_DATASET_URN,
                  getMockDatasetNameResponse(
                      "Expected Dataset Name", "full.name.Expected Dataset Name")));
      String qualifiedName =
          entityNameProvider.getQualifiedName(mock(OperationContext.class), TEST_DATASET_URN);
      Assert.assertEquals(qualifiedName, "full.name.Expected Dataset Name");
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown", e);
    }
  }

  private EntityResponse getMockDatasetNameResponse(String name) {
    return getMockDatasetNameResponse(name, null);
  }

  private EntityResponse getMockDatasetNameResponse(String name, String qualifiedName) {
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
                                    .setQualifiedName(qualifiedName, SetMode.IGNORE_NULL)
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

  private EntityResponse getMockOwnershipTypeResponse(String name) {
    return new EntityResponse()
        .setEntityName(OWNERSHIP_TYPE_ENTITY_NAME)
        .setUrn(TEST_OWNERSHIP_TYPE_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    OWNERSHIP_TYPE_INFO_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(OWNERSHIP_TYPE_INFO_ASPECT_NAME)
                        .setValue(new Aspect(new OwnershipTypeInfo().setName(name).data())))));
  }

  private EntityResponse getMockStructuredPropertyResponse(String name) {
    return new EntityResponse()
        .setEntityName(STRUCTURED_PROPERTY_ENTITY_NAME)
        .setUrn(TEST_STRUCTURED_PROPERTY_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
                        .setValue(
                            new Aspect(
                                new StructuredPropertyDefinition().setDisplayName(name).data())))));
  }

  private EntityResponse getMockGlossaryTermResponse(String name) {
    return new EntityResponse()
        .setEntityName(GLOSSARY_TERM_ENTITY_NAME)
        .setUrn(TEST_GLOSSARY_TERM_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    GLOSSARY_TERM_INFO_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(GLOSSARY_TERM_INFO_ASPECT_NAME)
                        .setValue(new Aspect(new GlossaryTermInfo().setName(name).data())))));
  }

  private EntityResponse getMockGlossaryNodeResponse(String name) {
    return new EntityResponse()
        .setEntityName(GLOSSARY_NODE_ENTITY_NAME)
        .setUrn(TEST_GLOSSARY_NODE_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    GLOSSARY_NODE_INFO_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(GLOSSARY_NODE_INFO_ASPECT_NAME)
                        .setValue(new Aspect(new GlossaryNodeInfo().setName(name).data())))));
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
