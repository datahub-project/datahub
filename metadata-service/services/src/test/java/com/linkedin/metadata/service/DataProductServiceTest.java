package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DataProductServiceTest {

  private static final Urn TEST_DATA_PRODUCT_URN =
      UrnUtils.getUrn("urn:li:dataProduct:test-product");
  private static final Urn TEST_RESOURCE_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");
  private static final Urn TEST_RESOURCE_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test2,PROD)");

  private static AspectRetriever mockAspectRetriever;
  private static OperationContext opContext;

  @BeforeClass
  public void init() {
    mockAspectRetriever = mock(AspectRetriever.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);
  }

  @Test
  public void testBatchAddToDataProduct() throws Exception {
    final EntityClient mockClient = mock(EntityClient.class);
    final GraphClient mockGraphClient = mock(GraphClient.class);
    final DataProductService service = new DataProductService(mockClient, mockGraphClient);

    final DataProductProperties existingProperties = new DataProductProperties();
    existingProperties.setName("Test Product");
    final DataProductAssociationArray existingAssociations = new DataProductAssociationArray();
    existingProperties.setAssets(existingAssociations);

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(existingProperties.data())))));

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.DATA_PRODUCT_ENTITY_NAME),
            eq(TEST_DATA_PRODUCT_URN),
            eq(ImmutableSet.of(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(entityResponse);

    when(mockClient.ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean()))
        .thenReturn(TEST_DATA_PRODUCT_URN.toString());

    final List<Urn> resources = ImmutableList.of(TEST_RESOURCE_URN_1, TEST_RESOURCE_URN_2);

    service.batchAddToDataProduct(opContext, TEST_DATA_PRODUCT_URN, resources);

    final ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), mcpCaptor.capture(), eq(false));

    final MetadataChangeProposal mcp = mcpCaptor.getValue();
    assertEquals(mcp.getEntityUrn(), TEST_DATA_PRODUCT_URN);
    assertEquals(mcp.getAspectName(), Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);

    final RecordTemplate updatedPropertiesRecord =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            DataProductProperties.class);
    final DataProductProperties updatedProperties = (DataProductProperties) updatedPropertiesRecord;
    assertEquals(updatedProperties.getAssets().size(), 2);
    assertTrue(
        updatedProperties.getAssets().stream()
            .anyMatch(assoc -> assoc.getDestinationUrn().equals(TEST_RESOURCE_URN_1)));
    assertTrue(
        updatedProperties.getAssets().stream()
            .anyMatch(assoc -> assoc.getDestinationUrn().equals(TEST_RESOURCE_URN_2)));
  }

  @Test
  public void testBatchAddToDataProductDoesNotDuplicateExisting() throws Exception {
    final EntityClient mockClient = mock(EntityClient.class);
    final GraphClient mockGraphClient = mock(GraphClient.class);
    final DataProductService service = new DataProductService(mockClient, mockGraphClient);

    final DataProductProperties existingProperties = new DataProductProperties();
    existingProperties.setName("Test Product");
    final DataProductAssociationArray existingAssociations = new DataProductAssociationArray();
    final DataProductAssociation existingAssociation = new DataProductAssociation();
    existingAssociation.setDestinationUrn(TEST_RESOURCE_URN_1);
    existingAssociation.setCreated(
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(opContext.getActorContext().getActorUrn()));
    existingAssociation.setLastModified(
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(opContext.getActorContext().getActorUrn()));
    existingAssociations.add(existingAssociation);
    existingProperties.setAssets(existingAssociations);

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(existingProperties.data())))));

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.DATA_PRODUCT_ENTITY_NAME),
            eq(TEST_DATA_PRODUCT_URN),
            eq(ImmutableSet.of(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(entityResponse);

    when(mockClient.ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean()))
        .thenReturn(TEST_DATA_PRODUCT_URN.toString());

    final List<Urn> resources = ImmutableList.of(TEST_RESOURCE_URN_1, TEST_RESOURCE_URN_2);

    service.batchAddToDataProduct(opContext, TEST_DATA_PRODUCT_URN, resources);

    final ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), mcpCaptor.capture(), eq(false));

    final MetadataChangeProposal mcp = mcpCaptor.getValue();
    final RecordTemplate updatedPropertiesRecord =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            DataProductProperties.class);
    final DataProductProperties updatedProperties = (DataProductProperties) updatedPropertiesRecord;

    assertEquals(updatedProperties.getAssets().size(), 2);
    assertEquals(
        updatedProperties.getAssets().stream()
            .filter(assoc -> assoc.getDestinationUrn().equals(TEST_RESOURCE_URN_1))
            .count(),
        1);
  }

  @Test
  public void testBatchRemoveFromDataProduct() throws Exception {
    final EntityClient mockClient = mock(EntityClient.class);
    final GraphClient mockGraphClient = mock(GraphClient.class);
    final DataProductService service = new DataProductService(mockClient, mockGraphClient);

    final DataProductProperties existingProperties = new DataProductProperties();
    existingProperties.setName("Test Product");
    final DataProductAssociationArray existingAssociations = new DataProductAssociationArray();

    final DataProductAssociation association1 = new DataProductAssociation();
    association1.setDestinationUrn(TEST_RESOURCE_URN_1);
    existingAssociations.add(association1);

    final DataProductAssociation association2 = new DataProductAssociation();
    association2.setDestinationUrn(TEST_RESOURCE_URN_2);
    existingAssociations.add(association2);

    existingProperties.setAssets(existingAssociations);

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(existingProperties.data())))));

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.DATA_PRODUCT_ENTITY_NAME),
            eq(TEST_DATA_PRODUCT_URN),
            eq(ImmutableSet.of(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(entityResponse);

    when(mockClient.ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean()))
        .thenReturn(TEST_DATA_PRODUCT_URN.toString());

    final List<Urn> resourcesToRemove = ImmutableList.of(TEST_RESOURCE_URN_1);

    service.batchRemoveFromDataProduct(opContext, TEST_DATA_PRODUCT_URN, resourcesToRemove);

    final ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), mcpCaptor.capture(), eq(false));

    final MetadataChangeProposal mcp = mcpCaptor.getValue();
    assertEquals(mcp.getEntityUrn(), TEST_DATA_PRODUCT_URN);
    assertEquals(mcp.getAspectName(), Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);

    final RecordTemplate updatedPropertiesRecord =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            DataProductProperties.class);
    final DataProductProperties updatedProperties = (DataProductProperties) updatedPropertiesRecord;

    assertEquals(updatedProperties.getAssets().size(), 1);
    assertEquals(updatedProperties.getAssets().get(0).getDestinationUrn(), TEST_RESOURCE_URN_2);
  }

  @Test
  public void testBatchRemoveFromDataProductRemovesMultiple() throws Exception {
    final EntityClient mockClient = mock(EntityClient.class);
    final GraphClient mockGraphClient = mock(GraphClient.class);
    final DataProductService service = new DataProductService(mockClient, mockGraphClient);

    final DataProductProperties existingProperties = new DataProductProperties();
    existingProperties.setName("Test Product");
    final DataProductAssociationArray existingAssociations = new DataProductAssociationArray();

    final DataProductAssociation association1 = new DataProductAssociation();
    association1.setDestinationUrn(TEST_RESOURCE_URN_1);
    existingAssociations.add(association1);

    final DataProductAssociation association2 = new DataProductAssociation();
    association2.setDestinationUrn(TEST_RESOURCE_URN_2);
    existingAssociations.add(association2);

    existingProperties.setAssets(existingAssociations);

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(existingProperties.data())))));

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.DATA_PRODUCT_ENTITY_NAME),
            eq(TEST_DATA_PRODUCT_URN),
            eq(ImmutableSet.of(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(entityResponse);

    when(mockClient.ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean()))
        .thenReturn(TEST_DATA_PRODUCT_URN.toString());

    final List<Urn> resourcesToRemove = ImmutableList.of(TEST_RESOURCE_URN_1, TEST_RESOURCE_URN_2);

    service.batchRemoveFromDataProduct(opContext, TEST_DATA_PRODUCT_URN, resourcesToRemove);

    final ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), mcpCaptor.capture(), eq(false));

    final MetadataChangeProposal mcp = mcpCaptor.getValue();
    final RecordTemplate updatedPropertiesRecord =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            DataProductProperties.class);
    final DataProductProperties updatedProperties = (DataProductProperties) updatedPropertiesRecord;

    assertEquals(updatedProperties.getAssets().size(), 0);
  }

  @Test
  public void testVerifyEntityExistsTrue() throws Exception {
    final EntityClient mockClient = mock(EntityClient.class);
    final GraphClient mockGraphClient = mock(GraphClient.class);
    final DataProductService service = new DataProductService(mockClient, mockGraphClient);

    when(mockClient.exists(any(OperationContext.class), eq(TEST_RESOURCE_URN_1))).thenReturn(true);

    final boolean result = service.verifyEntityExists(opContext, TEST_RESOURCE_URN_1);

    assertTrue(result);
    verify(mockClient, times(1)).exists(any(OperationContext.class), eq(TEST_RESOURCE_URN_1));
  }

  @Test
  public void testVerifyEntityExistsFalse() throws Exception {
    final EntityClient mockClient = mock(EntityClient.class);
    final GraphClient mockGraphClient = mock(GraphClient.class);
    final DataProductService service = new DataProductService(mockClient, mockGraphClient);

    when(mockClient.exists(any(OperationContext.class), eq(TEST_RESOURCE_URN_1))).thenReturn(false);

    final boolean result = service.verifyEntityExists(opContext, TEST_RESOURCE_URN_1);

    assertFalse(result);
    verify(mockClient, times(1)).exists(any(OperationContext.class), eq(TEST_RESOURCE_URN_1));
  }
}
