package com.linkedin.datahub.upgrade.system.browsepaths;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillIcebergBrowsePathsV2StepTest {

  private BackfillIcebergBrowsePathsV2Step step;
  private EntityService<?> mockEntityService;
  private SearchService mockSearchService;
  private OperationContext mockOpContext;
  private UpgradeContext mockUpgradeContext;

  private static final String CONTAINER_URN = "urn:li:container:iceberg__test_wh_0.default.ns0";
  private static final String PLATFORM_INSTANCE_URN =
      "urn:li:dataPlatformInstance:(urn:li:dataPlatform:iceberg,test_wh_0)";
  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:iceberg,test_wh_0.100df154-cab9-4046-a900-753e996b5001,PROD)";

  @BeforeMethod
  public void setUp() {
    mockEntityService = mock(EntityService.class);
    mockSearchService = mock(SearchService.class);
    mockOpContext = TestOperationContexts.systemContextNoValidate();
    mockUpgradeContext = mock(UpgradeContext.class);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);

    step =
        new BackfillIcebergBrowsePathsV2Step(
            mockOpContext, mockEntityService, mockSearchService, 10);
  }

  @Test
  public void testId() {
    assertEquals(step.id(), "BackfillIcebergBrowsePathsV2Step");
  }

  @Test
  public void testIsOptional() {
    assertTrue(step.isOptional());
  }

  @Test
  public void testSkipWhenPreviouslyRun() {
    // Mock that the upgrade was previously run
    when(mockEntityService.exists(
            eq(mockOpContext),
            any(Urn.class),
            eq(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(true);

    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSkipWhenNotPreviouslyRun() {
    // Mock that the upgrade was not previously run
    when(mockEntityService.exists(
            eq(mockOpContext),
            any(Urn.class),
            eq(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(false);

    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testPlatformUrn() {
    Urn platformUrn = step.platformUrn();
    assertEquals(platformUrn.toString(), "urn:li:dataPlatform:iceberg");
  }

  @Test
  public void testPlatformInstanceUrnFromContainerUrn() {
    Urn containerUrn = UrnUtils.getUrn(CONTAINER_URN);
    Urn platformInstanceUrn = step.platformInstanceUrnFromContainerUrn(containerUrn);

    assertEquals(platformInstanceUrn.toString(), PLATFORM_INSTANCE_URN);
  }

  @Test
  public void testNamespaceNameFromContainerUrn() {
    Urn containerUrn = UrnUtils.getUrn(CONTAINER_URN);
    String namespace =
        BackfillIcebergBrowsePathsV2StepTest.namespaceNameFromContainerUrn(containerUrn);

    assertEquals(namespace, "default.ns0");
  }

  @Test
  public void testExecutableSuccess() {
    // Mock search results for containers
    ScrollResult mockScrollResult = mock(ScrollResult.class);
    Urn containerUrn = UrnUtils.getUrn(CONTAINER_URN);

    // Create actual SearchEntity objects instead of mocks
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(containerUrn);

    when(mockScrollResult.getNumEntities()).thenReturn(1);
    SearchEntityArray searchEntityArray = new SearchEntityArray();
    searchEntityArray.add(searchEntity);
    when(mockScrollResult.getEntities()).thenReturn(searchEntityArray);
    when(mockScrollResult.getScrollId()).thenReturn(null); // No more results

    // Mock empty dataset scroll result
    ScrollResult mockDatasetScrollResult = mock(ScrollResult.class);
    SearchEntityArray emptySearchEntityArray = new SearchEntityArray();
    when(mockDatasetScrollResult.getNumEntities()).thenReturn(0);
    when(mockDatasetScrollResult.getEntities()).thenReturn(emptySearchEntityArray);
    when(mockDatasetScrollResult.getScrollId()).thenReturn(null);

    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class),
            eq(Collections.singletonList(Constants.CONTAINER_ENTITY_NAME)),
            eq("dataplatform:iceberg"),
            eq(null),
            eq(null),
            eq(null),
            eq(null),
            eq(10)))
        .thenReturn(mockScrollResult);

    // Mock search results for datasets
    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class),
            eq(Collections.singletonList(Constants.DATASET_ENTITY_NAME)),
            eq("dataplatform:iceberg"),
            eq(null),
            eq(null),
            eq(null),
            eq(null),
            eq(10)))
        .thenReturn(mockDatasetScrollResult);

    // Mock entity service calls
    when(mockEntityService.getLatestAspect(
            eq(mockOpContext), eq(containerUrn), eq(Constants.BROWSE_PATHS_V2_ASPECT_NAME)))
        .thenReturn(null);

    // Execute the step
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // Verify the result
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that the upgrade result was set
    verify(mockEntityService, times(1))
        .ingestProposal(eq(mockOpContext), any(MetadataChangeProposal.class), any(), eq(true));
  }

  @Test
  public void testIngestBrowsePathsV2ForContainer() throws Exception {
    Urn containerUrn = UrnUtils.getUrn(CONTAINER_URN);

    // Mock entity service calls
    when(mockEntityService.getLatestAspect(
            eq(mockOpContext), eq(containerUrn), eq(Constants.BROWSE_PATHS_V2_ASPECT_NAME)))
        .thenReturn(null);

    // Use reflection to access private method
    java.lang.reflect.Method method =
        BackfillIcebergBrowsePathsV2Step.class.getDeclaredMethod(
            "ingestBrowsePathsV2ForContainer",
            OperationContext.class,
            Urn.class,
            com.linkedin.common.AuditStamp.class);
    method.setAccessible(true);

    com.linkedin.common.AuditStamp auditStamp =
        new com.linkedin.common.AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    method.invoke(step, mockOpContext, containerUrn, auditStamp);

    // Verify that a proposal was ingested
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService)
        .ingestProposal(eq(mockOpContext), proposalCaptor.capture(), eq(auditStamp), eq(true));

    MetadataChangeProposal capturedProposal = proposalCaptor.getValue();
    assertEquals(capturedProposal.getEntityUrn(), containerUrn);
    assertEquals(capturedProposal.getEntityType(), Constants.CONTAINER_ENTITY_NAME);
    assertEquals(capturedProposal.getAspectName(), Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    assertEquals(capturedProposal.getChangeType(), ChangeType.UPSERT);
  }

  @Test
  public void testIngestBrowsePathsV2ForDataset() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    Urn containerUrn = UrnUtils.getUrn(CONTAINER_URN);

    // Mock container aspect
    Container mockContainer = new Container();
    mockContainer.setContainer(containerUrn);
    RecordTemplate mockContainerAspect = mock(RecordTemplate.class);
    when(mockContainerAspect.data()).thenReturn(mockContainer.data());

    // Mock entity service calls
    when(mockEntityService.getLatestAspect(
            eq(mockOpContext), eq(datasetUrn), eq(Constants.BROWSE_PATHS_V2_ASPECT_NAME)))
        .thenReturn(null);
    when(mockEntityService.getLatestAspect(
            eq(mockOpContext), eq(datasetUrn), eq(Constants.CONTAINER_ASPECT_NAME)))
        .thenReturn(mockContainerAspect);

    // Use reflection to access private method
    java.lang.reflect.Method method =
        BackfillIcebergBrowsePathsV2Step.class.getDeclaredMethod(
            "ingestBrowsePathsV2ForDataset",
            OperationContext.class,
            Urn.class,
            com.linkedin.common.AuditStamp.class);
    method.setAccessible(true);

    com.linkedin.common.AuditStamp auditStamp =
        new com.linkedin.common.AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    method.invoke(step, mockOpContext, datasetUrn, auditStamp);

    // Verify that a proposal was ingested
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService)
        .ingestProposal(eq(mockOpContext), proposalCaptor.capture(), eq(auditStamp), eq(true));

    MetadataChangeProposal capturedProposal = proposalCaptor.getValue();
    assertEquals(capturedProposal.getEntityUrn(), datasetUrn);
    assertEquals(capturedProposal.getEntityType(), Constants.DATASET_ENTITY_NAME);
    assertEquals(capturedProposal.getAspectName(), Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    assertEquals(capturedProposal.getChangeType(), ChangeType.UPSERT);
  }

  @Test
  public void testAddBrowsePathsForContainer() throws Exception {
    BrowsePathEntryArray browsePathsArray = new BrowsePathEntryArray();
    String namespaceUrnPrefix = "urn:li:container:iceberg__test_wh_0";
    String[] levels = {"default", "ns0"};

    // Use reflection to access private method
    java.lang.reflect.Method method =
        BackfillIcebergBrowsePathsV2Step.class.getDeclaredMethod(
            "addBrowsePathsForContainer", String.class, String[].class, BrowsePathEntryArray.class);
    method.setAccessible(true);

    method.invoke(step, namespaceUrnPrefix, levels, browsePathsArray);

    assertEquals(browsePathsArray.size(), 2);

    BrowsePathEntry firstEntry = browsePathsArray.get(0);
    assertEquals(firstEntry.getId(), "urn:li:container:iceberg__test_wh_0.default");
    assertEquals(firstEntry.getUrn().toString(), "urn:li:container:iceberg__test_wh_0.default");

    BrowsePathEntry secondEntry = browsePathsArray.get(1);
    assertEquals(secondEntry.getId(), "urn:li:container:iceberg__test_wh_0.default.ns0");
    assertEquals(
        secondEntry.getUrn().toString(), "urn:li:container:iceberg__test_wh_0.default.ns0");
  }

  @Test
  public void testBackfillBrowsePathsV2WithNoResults() {
    // Mock empty search results
    ScrollResult mockScrollResult = mock(ScrollResult.class);
    when(mockScrollResult.getNumEntities()).thenReturn(0);
    SearchEntityArray emptySearchEntityArray = new SearchEntityArray();
    when(mockScrollResult.getEntities()).thenReturn(emptySearchEntityArray);

    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class), any(), anyString(), any(), any(), any(), any(), anyInt()))
        .thenReturn(mockScrollResult);

    // Use package-scoped method directly
    com.linkedin.common.AuditStamp auditStamp =
        new com.linkedin.common.AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    String result = step.backfillBrowsePathsV2(Constants.CONTAINER_ENTITY_NAME, auditStamp, null);
    assertEquals(result, null);
  }

  @Test
  public void testBackfillBrowsePathsV2WithResults() {
    // Mock search results
    ScrollResult mockScrollResult = mock(ScrollResult.class);
    Urn containerUrn = UrnUtils.getUrn(CONTAINER_URN);

    // Create actual SearchEntity objects instead of mocks
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(containerUrn);

    when(mockScrollResult.getNumEntities()).thenReturn(1);
    SearchEntityArray searchEntityArray2 = new SearchEntityArray();
    searchEntityArray2.add(searchEntity);
    when(mockScrollResult.getEntities()).thenReturn(searchEntityArray2);
    when(mockScrollResult.getScrollId()).thenReturn("next-scroll-id");

    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class), any(), anyString(), any(), any(), any(), any(), anyInt()))
        .thenReturn(mockScrollResult);

    // Mock entity service calls
    when(mockEntityService.getLatestAspect(
            eq(mockOpContext), eq(containerUrn), eq(Constants.BROWSE_PATHS_V2_ASPECT_NAME)))
        .thenReturn(null);

    // Use package-scoped method directly
    com.linkedin.common.AuditStamp auditStamp =
        new com.linkedin.common.AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    String result = step.backfillBrowsePathsV2(Constants.CONTAINER_ENTITY_NAME, auditStamp, null);
    assertEquals(result, "next-scroll-id");

    // Verify that a proposal was ingested
    verify(mockEntityService)
        .ingestProposal(
            eq(mockOpContext), any(MetadataChangeProposal.class), eq(auditStamp), eq(true));
  }

  @Test
  public void testBackfillBrowsePathsV2WithException() {
    // Mock search results with entity that will cause exception
    ScrollResult mockScrollResult = mock(ScrollResult.class);
    Urn invalidUrn = UrnUtils.getUrn("urn:li:container:invalid");

    // Create actual SearchEntity objects instead of mocks
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(invalidUrn);

    when(mockScrollResult.getNumEntities()).thenReturn(1);
    SearchEntityArray searchEntityArray3 = new SearchEntityArray();
    searchEntityArray3.add(searchEntity);
    when(mockScrollResult.getEntities()).thenReturn(searchEntityArray3);
    when(mockScrollResult.getScrollId()).thenReturn(null);

    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class), any(), anyString(), any(), any(), any(), any(), anyInt()))
        .thenReturn(mockScrollResult);

    // Use package-scoped method directly
    com.linkedin.common.AuditStamp auditStamp =
        new com.linkedin.common.AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    String result = step.backfillBrowsePathsV2(Constants.CONTAINER_ENTITY_NAME, auditStamp, null);
    assertEquals(result, null);

    // Verify that no proposal was ingested due to exception
    verify(mockEntityService, times(0))
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(),
            any(Boolean.class));
  }

  // Helper method to access package-scoped static method
  private static String namespaceNameFromContainerUrn(Urn urn) {
    return BackfillIcebergBrowsePathsV2Step.namespaceNameFromContainerUrn(urn);
  }
}
