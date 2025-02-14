package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.DataHubRestCatalog.*;
import static io.datahubproject.iceberg.catalog.Utils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubRestCatalogTest {

  @Mock private EntityService entityService;

  @Mock private EntitySearchService searchService;

  @Mock private OperationContext operationContext;

  @Mock private DataHubIcebergWarehouse warehouse;

  @Mock private CredentialProvider credentialProvider;

  private DataHubRestCatalog catalog;

  private final Urn testUser = new CorpuserUrn("urn:li:corpuser:testUser");

  private final String platformInstanceName = "test-platform";

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(warehouse.getPlatformInstance()).thenReturn(platformInstanceName);
    String warehouseRoot = "s3://data/warehouse/";
    when(warehouse.getDataRoot()).thenReturn(warehouseRoot);
    catalog =
        new DataHubRestCatalog(
            entityService, searchService, operationContext, warehouse, credentialProvider);

    ActorContext actorContext = mock(ActorContext.class);
    when(operationContext.getActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(testUser);
  }

  @Test
  public void testCreateNamespace_SingleLevel() throws Exception {
    Namespace namespace = Namespace.of("db1");
    Map<String, String> properties = Map.of("a", "b");

    Urn containerUrn = containerUrn(platformInstanceName, namespace);

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(CONTAINER_PROPERTIES_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(
                serializeAspect(
                    new ContainerProperties()
                        .setName(namespace.levels()[namespace.length() - 1])
                        .setCustomProperties(new StringMap(properties))))
            .setChangeType(ChangeType.CREATE_ENTITY));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(false)))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(dataPlatformInstance()))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(SUB_TYPES_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new SubTypes().setTypeNames(new StringArray("Namespace"))))
            .setChangeType(ChangeType.CREATE));

    AspectsBatchImpl markerBatch = mock(AspectsBatchImpl.class);

    when(entityService.ingestProposal(same(operationContext), same(markerBatch), eq(false)))
        .thenReturn(List.of());

    AspectsBatchImpl.AspectsBatchImplBuilder builder =
        mock(AspectsBatchImpl.AspectsBatchImplBuilder.class);

    when(builder.mcps(eq(expectedMcps), any(AuditStamp.class), any())).thenReturn(builder);
    when(builder.build()).thenReturn(markerBatch);

    try (MockedStatic<AspectsBatchImpl> stsClientMockedStatic =
        mockStatic(AspectsBatchImpl.class)) {
      stsClientMockedStatic.when(AspectsBatchImpl::builder).thenReturn(builder);
      catalog.createNamespace(namespace, properties);
    }

    verify(builder).mcps(eq(expectedMcps), any(AuditStamp.class), any());
    verify(entityService).ingestProposal(same(operationContext), same(markerBatch), eq(false));
  }

  @Test
  public void testCreateNamespace_MultiLevel() throws Exception {
    Namespace namespace = Namespace.of("db1", "schema1");
    Map<String, String> properties = Map.of("a", "b");
    Urn containerUrn = containerUrn(platformInstanceName, namespace);
    Urn parent = containerUrn(platformInstanceName, Namespace.of("db1"));

    when(entityService.exists(eq(operationContext), eq(parent))).thenReturn(true);

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(CONTAINER_PROPERTIES_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(
                serializeAspect(
                    new ContainerProperties()
                        .setName(namespace.levels()[namespace.length() - 1])
                        .setCustomProperties(new StringMap(properties))))
            .setChangeType(ChangeType.CREATE_ENTITY));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(false)))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(CONTAINER_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Container().setContainer(parent)))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(dataPlatformInstance()))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(SUB_TYPES_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new SubTypes().setTypeNames(new StringArray("Namespace"))))
            .setChangeType(ChangeType.CREATE));

    AspectsBatchImpl markerBatch = mock(AspectsBatchImpl.class);

    when(entityService.ingestProposal(same(operationContext), same(markerBatch), eq(false)))
        .thenReturn(List.of());

    AspectsBatchImpl.AspectsBatchImplBuilder builder =
        mock(AspectsBatchImpl.AspectsBatchImplBuilder.class);

    when(builder.mcps(eq(expectedMcps), any(AuditStamp.class), any())).thenReturn(builder);
    when(builder.build()).thenReturn(markerBatch);

    try (MockedStatic<AspectsBatchImpl> stsClientMockedStatic =
        mockStatic(AspectsBatchImpl.class)) {
      stsClientMockedStatic.when(AspectsBatchImpl::builder).thenReturn(builder);
      catalog.createNamespace(namespace, properties);
    }

    verify(builder).mcps(eq(expectedMcps), any(AuditStamp.class), any());
    verify(entityService).ingestProposal(same(operationContext), same(markerBatch), eq(false));
    verify(entityService).exists(eq(operationContext), eq(parent));
  }

  private DataPlatformInstance dataPlatformInstance() {
    DataPlatformInstance platformInstance = new DataPlatformInstance();
    platformInstance.setPlatform(platformUrn());
    platformInstance.setInstance(platformInstanceUrn(platformInstanceName));
    return platformInstance;
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testCreateNamespace_MultiLevel_ParentMissing() throws Exception {
    Namespace namespace = Namespace.of("db1", "schema1");
    Map<String, String> properties = Map.of();

    when(entityService.exists(eq(operationContext), any(Urn.class))).thenReturn(false);

    // Act - should throw exception
    catalog.createNamespace(namespace, properties);
  }

  @Test
  public void testLoadNamespaceMetadata_Exists() throws Exception {
    Namespace namespace = Namespace.of("db1", "schema1");
    ContainerProperties containerProperties =
        new ContainerProperties().setCustomProperties(new StringMap());
    Urn urn = Urn.createFromString("urn:li:container:iceberg__test-platform.db1.schema1");
    when(entityService.getLatestAspect(
            eq(operationContext), eq(urn), eq(CONTAINER_PROPERTIES_ASPECT_NAME)))
        .thenReturn(containerProperties);

    Map<String, String> metadata = catalog.loadNamespaceMetadata(namespace);

    assertNotNull(metadata);
    assertTrue(metadata.isEmpty());
    verify(entityService)
        .getLatestAspect(eq(operationContext), eq(urn), eq(CONTAINER_PROPERTIES_ASPECT_NAME));
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testLoadNamespaceMetadata_NotExists() throws Exception {
    Namespace namespace = Namespace.of("db1", "schema1");
    Urn urn = Urn.createFromString("urn:li:container:iceberg__test-platform.db1.schema1");
    when(entityService.getLatestAspect(
            eq(operationContext), eq(urn), eq(CONTAINER_PROPERTIES_ASPECT_NAME)))
        .thenReturn(null);

    // Act & Assert - should throw exception
    catalog.loadNamespaceMetadata(namespace);
  }

  @Test
  public void testDropTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db1", "table1");
    when(warehouse.deleteDataset(eq(tableIdentifier))).thenReturn(true);

    boolean result = catalog.dropTable(tableIdentifier, false);

    assertTrue(result);
    verify(warehouse).deleteDataset(eq(tableIdentifier));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testDropTable_WithPurgeThrows() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db1", "table1");

    catalog.dropTable(tableIdentifier, true);
  }

  @Test
  public void testDropTable_NonExistentTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db1", "table1");
    when(entityService.exists(eq(operationContext), (Urn) any())).thenReturn(false);

    boolean result = catalog.dropTable(tableIdentifier, false);

    // Assert
    assertFalse(result);
    verify(entityService, never()).deleteUrn(any(), any());
  }

  @Test
  public void testDefaultWarehouseLocation() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db1", "table1");

    String location = catalog.defaultWarehouseLocation(tableIdentifier);

    assertEquals(location, "s3://data/warehouse/db1/table1");
  }

  @Test
  public void testDefaultWarehouseLocationWithoutTrailingSlash() {
    String warehouseRoot = "s3://data/warehouse";
    when(warehouse.getDataRoot()).thenReturn(warehouseRoot);
    DataHubRestCatalog testCatalog =
        new DataHubRestCatalog(
            entityService, searchService, operationContext, warehouse, credentialProvider);
    String warehouseLocation =
        testCatalog.defaultWarehouseLocation(TableIdentifier.of("db1", "table1"));
    assertEquals(warehouseLocation, "s3://data/warehouse/db1/table1");
  }

  @Test
  public void testListNamespaces_EmptyNamespace() throws Exception {
    // Test for root level namespace listing
    Namespace emptyNamespace = Namespace.empty();
    SearchResult mockResult = mock(SearchResult.class);
    List<SearchEntity> entitiesList =
        Arrays.asList(
            createSearchEntity("urn:li:container:iceberg__ns1"),
            createSearchEntity("urn:li:container:iceberg__ns2"));
    SearchEntityArray entities = new SearchEntityArray();
    entities.addAll(entitiesList);
    when(mockResult.getEntities()).thenReturn(entities);
    when(mockResult.getNumEntities()).thenReturn(2);
    when(searchService.search(
            eq(operationContext), any(), eq("*"), any(), any(), eq(0), eq(PAGE_SIZE)))
        .thenReturn(mockResult);

    List<Namespace> result = catalog.listNamespaces(emptyNamespace);

    assertEquals(result.size(), 2);
    assertEquals(result.get(0), Namespace.of("ns1"));
    assertEquals(result.get(1), Namespace.of("ns2"));
  }

  @Test
  public void testListNamespaces_NestedNamespace() throws Exception {
    Namespace parentNamespace = Namespace.of("parent");
    SearchResult mockResult = mock(SearchResult.class);
    List<SearchEntity> entitiesList =
        Arrays.asList(
            createSearchEntity("urn:li:container:iceberg__parent.ns1"),
            createSearchEntity("urn:li:container:iceberg__parent.ns2"));
    SearchEntityArray entities = new SearchEntityArray();
    entities.addAll(entitiesList);
    when(mockResult.getEntities()).thenReturn(entities);
    when(mockResult.getNumEntities()).thenReturn(2);
    when(searchService.search(
            eq(operationContext), any(), eq("*"), any(), any(), eq(0), eq(PAGE_SIZE)))
        .thenReturn(mockResult);

    List<Namespace> result = catalog.listNamespaces(parentNamespace);

    assertEquals(result.size(), 2);
    assertEquals(result.get(0), Namespace.of("parent", "ns1"));
    assertEquals(result.get(1), Namespace.of("parent", "ns2"));
  }

  @Test
  public void testDropNamespace() throws Exception {
    Namespace namespace = Namespace.of("db1");
    boolean result = catalog.dropNamespace(namespace);
    assertFalse(result); // Current implementation always returns false
  }

  @Test
  public void testListTables() throws Exception {
    Namespace namespace = Namespace.of("ns1");
    List<SearchEntity> entitiesList =
        Arrays.asList(
            createSearchEntity("urn:li:dataset:iceberg__ns1.table1"),
            createSearchEntity("urn:li:dataset:iceberg__ns1.table2"));
    SearchEntityArray entities = new SearchEntityArray();
    entities.addAll(entitiesList);
    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(entities);
    searchResult.setNumEntities(2);

    // Mock aspect retrieval
    Map<Urn, List<RecordTemplate>> aspects = new HashMap<>();
    for (SearchEntity entity : entities) {
      DatasetProperties props =
          new DatasetProperties()
              .setQualifiedName(
                  "warehouse.ns1.table"
                      + entity
                          .getEntity()
                          .toString()
                          .charAt(entity.getEntity().toString().length() - 1));
      aspects.put(entity.getEntity(), Arrays.asList(props));
    }
    when(entityService.getLatestAspects(eq(operationContext), any(), any(), eq(false)))
        .thenReturn(aspects);
    when(searchService.search(
            eq(operationContext), any(), any(), any(), any(), eq(0), eq(PAGE_SIZE)))
        .thenReturn(searchResult);

    List<TableIdentifier> result = catalog.listTables(namespace);

    assertEquals(result.size(), 2);
    assertEquals(result.get(0), TableIdentifier.of("ns1", "table1"));
    assertEquals(result.get(1), TableIdentifier.of("ns1", "table2"));
  }

  @Test
  public void testListViews() throws Exception {
    Namespace namespace = Namespace.of("ns1");
    List<SearchEntity> entitiesList =
        Arrays.asList(
            createSearchEntity("urn:li:dataset:iceberg__ns1.view1"),
            createSearchEntity("urn:li:dataset:iceberg__ns1.view2"));
    SearchEntityArray entities = new SearchEntityArray();
    entities.addAll(entitiesList);
    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(entities);
    searchResult.setNumEntities(2);

    // Mock aspect retrieval
    Map<Urn, List<RecordTemplate>> aspects = new HashMap<>();
    for (SearchEntity entity : entities) {
      DatasetProperties props =
          new DatasetProperties()
              .setQualifiedName(
                  "warehouse.ns1.view"
                      + entity
                          .getEntity()
                          .toString()
                          .charAt(entity.getEntity().toString().length() - 1));
      aspects.put(entity.getEntity(), Arrays.asList(props));
    }
    when(entityService.getLatestAspects(eq(operationContext), any(), any(), eq(false)))
        .thenReturn(aspects);

    when(searchService.search(
            eq(operationContext), any(), any(), any(), any(), eq(0), eq(PAGE_SIZE)))
        .thenReturn(searchResult);

    List<TableIdentifier> result = catalog.listViews(namespace);

    assertEquals(result.size(), 2);
    assertEquals(result.get(0), TableIdentifier.of("ns1", "view1"));
    assertEquals(result.get(1), TableIdentifier.of("ns1", "view2"));
  }

  @Test
  public void testDropView() throws Exception {
    TableIdentifier viewIdentifier = TableIdentifier.of("ns1", "view1");
    when(warehouse.deleteDataset(eq(viewIdentifier))).thenReturn(true);

    boolean result = catalog.dropView(viewIdentifier);

    assertTrue(result);
    verify(warehouse).deleteDataset(eq(viewIdentifier));
  }

  @Test
  public void testUpdateNamespaceProperties() throws Exception {
    Namespace namespace = Namespace.of("ns1");
    Map<String, String> existingProps = new HashMap<>();
    existingProps.put("existing1", "value1");
    existingProps.put("toRemove1", "value2");

    when(entityService.getLatestAspect(
            eq(operationContext), any(), eq(CONTAINER_PROPERTIES_ASPECT_NAME)))
        .thenReturn(new ContainerProperties().setCustomProperties(new StringMap(existingProps)));

    UpdateNamespacePropertiesRequest request =
        UpdateNamespacePropertiesRequest.builder()
            .update("new1", "newValue1")
            .remove("toRemove1")
            .build();

    Urn containerUrn = containerUrn(platformInstanceName, namespace);

    ContainerProperties expectedProps =
        new ContainerProperties()
            .setName("ns1")
            .setCustomProperties(
                new StringMap(
                    Map.of(
                        "existing1", "value1",
                        "new1", "newValue1")));

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(containerUrn)
            .setEntityType(CONTAINER_ENTITY_NAME)
            .setAspectName(CONTAINER_PROPERTIES_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(expectedProps))
            .setChangeType(ChangeType.UPDATE));

    AspectsBatchImpl markerBatch = mock(AspectsBatchImpl.class);

    when(entityService.ingestProposal(same(operationContext), same(markerBatch), eq(false)))
        .thenReturn(List.of());

    AspectsBatchImpl.AspectsBatchImplBuilder builder =
        mock(AspectsBatchImpl.AspectsBatchImplBuilder.class);

    when(builder.mcps(eq(expectedMcps), any(AuditStamp.class), any())).thenReturn(builder);
    when(builder.build()).thenReturn(markerBatch);

    try (MockedStatic<AspectsBatchImpl> stsClientMockedStatic =
        mockStatic(AspectsBatchImpl.class)) {
      stsClientMockedStatic.when(AspectsBatchImpl::builder).thenReturn(builder);
      UpdateNamespacePropertiesResponse response =
          catalog.updateNamespaceProperties(namespace, request);

      assertTrue(response.removed().contains("toRemove1"));
      assertTrue(response.updated().contains("new1"));
      assertTrue(response.missing().isEmpty());
    }

    verify(builder).mcps(eq(expectedMcps), any(AuditStamp.class), any());
    verify(entityService).ingestProposal(same(operationContext), same(markerBatch), eq(false));
  }

  // Helper method for creating mock search entities
  private SearchEntity createSearchEntity(String urn) throws Exception {
    SearchEntity entity = new SearchEntity();
    entity.setEntity(Urn.createFromString(urn));
    return entity;
  }
}
