package com.linkedin.datahub.upgrade.shared;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticSearchUpgradeUtilsTest {

  private GraphService mockGraphService;
  private EntitySearchService mockEntitySearchService;
  private SystemMetadataService mockSystemMetadataService;
  private TimeseriesAspectService mockTimeseriesAspectService;
  private AspectDao mockAspectDao;

  @BeforeMethod
  public void setUp() {
    mockGraphService = mock(GraphService.class);
    mockEntitySearchService = mock(EntitySearchService.class);
    mockSystemMetadataService = mock(SystemMetadataService.class);
    mockTimeseriesAspectService = mock(TimeseriesAspectService.class);
    mockAspectDao = mock(AspectDao.class);
  }

  @Test
  public void testCreateElasticSearchIndexedServicesAllImplement() {
    // Create mock services that implement ElasticSearchIndexed
    GraphService mockGraphService =
        mock(GraphService.class, withSettings().extraInterfaces(ElasticSearchIndexed.class));
    EntitySearchService mockEntitySearchService =
        mock(EntitySearchService.class, withSettings().extraInterfaces(ElasticSearchIndexed.class));
    SystemMetadataService mockSystemMetadataService =
        mock(
            SystemMetadataService.class,
            withSettings().extraInterfaces(ElasticSearchIndexed.class));
    TimeseriesAspectService mockTimeseriesAspectService =
        mock(
            TimeseriesAspectService.class,
            withSettings().extraInterfaces(ElasticSearchIndexed.class));

    List<ElasticSearchIndexed> result =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            mockGraphService,
            mockEntitySearchService,
            mockSystemMetadataService,
            mockTimeseriesAspectService);

    assertNotNull(result);
    assertEquals(result.size(), 4);
  }

  @Test
  public void testCreateElasticSearchIndexedServicesNoneImplement() {
    // Create mock services that don't implement ElasticSearchIndexed
    GraphService mockGraphService = mock(GraphService.class);
    EntitySearchService mockEntitySearchService = mock(EntitySearchService.class);
    SystemMetadataService mockSystemMetadataService = mock(SystemMetadataService.class);
    TimeseriesAspectService mockTimeseriesAspectService = mock(TimeseriesAspectService.class);

    List<ElasticSearchIndexed> result =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            mockGraphService,
            mockEntitySearchService,
            mockSystemMetadataService,
            mockTimeseriesAspectService);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testCreateElasticSearchIndexedServicesMixed() {
    // Create a mix of services - some implement ElasticSearchIndexed, some don't
    GraphService mockGraphService =
        mock(GraphService.class, withSettings().extraInterfaces(ElasticSearchIndexed.class));
    EntitySearchService mockEntitySearchService =
        mock(EntitySearchService.class, withSettings().extraInterfaces(ElasticSearchIndexed.class));
    SystemMetadataService mockSystemMetadataService = mock(SystemMetadataService.class);
    TimeseriesAspectService mockTimeseriesAspectService = mock(TimeseriesAspectService.class);

    List<ElasticSearchIndexed> result =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            mockGraphService,
            mockEntitySearchService,
            mockSystemMetadataService,
            mockTimeseriesAspectService);

    assertNotNull(result);
    assertEquals(result.size(), 2);
  }

  @Test
  public void testCreateElasticSearchIndexedServicesWithNulls() {
    // Test with null services
    EntitySearchService mockEntitySearchService =
        mock(EntitySearchService.class, withSettings().extraInterfaces(ElasticSearchIndexed.class));
    SystemMetadataService mockSystemMetadataService =
        mock(
            SystemMetadataService.class,
            withSettings().extraInterfaces(ElasticSearchIndexed.class));

    List<ElasticSearchIndexed> result =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            null, mockEntitySearchService, mockSystemMetadataService, null);

    assertNotNull(result);
    assertEquals(result.size(), 2); // null services are filtered out
  }

  @Test
  public void testGetActiveStructuredPropertiesDefinitions() {
    // Create a fresh mock for this test
    AspectDao freshMockAspectDao = mock(AspectDao.class);

    // Mock EntityAspect for active structured property
    EntityAspect activeAspect = mock(EntityAspect.class);
    Urn activeUrn = UrnUtils.getUrn("urn:li:structuredProperty:active");
    StructuredPropertyDefinition activeDefinition = new StructuredPropertyDefinition();
    activeDefinition.setQualifiedName("active.property");

    Status activeStatus = new Status();
    activeStatus.setRemoved(false);

    when(activeAspect.getUrn()).thenReturn(activeUrn.toString());
    when(activeAspect.getMetadata()).thenReturn(RecordUtils.toJsonString(activeDefinition));

    // Mock EntityAspect for removed structured property
    EntityAspect removedAspect = mock(EntityAspect.class);
    Urn removedUrn = UrnUtils.getUrn("urn:li:structuredProperty:removed");
    StructuredPropertyDefinition removedDefinition = new StructuredPropertyDefinition();
    removedDefinition.setQualifiedName("removed.property");

    Status removedStatus = new Status();
    removedStatus.setRemoved(true);

    when(removedAspect.getUrn()).thenReturn(removedUrn.toString());
    when(removedAspect.getMetadata()).thenReturn(RecordUtils.toJsonString(removedStatus));

    // Mock AspectDao to return streams for both calls (removed properties and all properties)
    when(freshMockAspectDao.streamAspects(anyString(), anyString()))
        .thenReturn(Stream.of(removedAspect)) // First call for removed properties
        .thenReturn(Stream.of(activeAspect)); // Second call for all properties (only active)

    Set<Pair<Urn, StructuredPropertyDefinition>> result =
        ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(freshMockAspectDao);

    assertNotNull(result);
    assertEquals(result.size(), 1);

    // Verify only active property is returned
    Pair<Urn, StructuredPropertyDefinition> activePair = result.iterator().next();
    assertEquals(activePair.getFirst(), activeUrn);
    assertEquals(activePair.getSecond().getQualifiedName(), activeDefinition.getQualifiedName());
  }

  @Test
  public void testGetActiveStructuredPropertiesDefinitionsEmpty() {
    // Create a fresh mock for this test
    AspectDao freshMockAspectDao = mock(AspectDao.class);

    // Mock AspectDao to return empty streams for both calls (removed properties and all properties)
    when(freshMockAspectDao.streamAspects(anyString(), anyString()))
        .thenReturn(Stream.empty())
        .thenReturn(Stream.empty());

    Set<Pair<Urn, StructuredPropertyDefinition>> result =
        ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(freshMockAspectDao);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetActiveStructuredPropertiesDefinitionsAllRemoved() {
    // Create a fresh mock for this test
    AspectDao freshMockAspectDao = mock(AspectDao.class);

    // Mock EntityAspect for removed structured property
    EntityAspect removedAspect = mock(EntityAspect.class);
    Urn removedUrn = UrnUtils.getUrn("urn:li:structuredProperty:removed");
    StructuredPropertyDefinition removedDefinition = new StructuredPropertyDefinition();
    removedDefinition.setQualifiedName("removed.property");

    Status removedStatus = new Status();
    removedStatus.setRemoved(true);

    when(removedAspect.getUrn()).thenReturn(removedUrn.toString());
    when(removedAspect.getMetadata()).thenReturn(RecordUtils.toJsonString(removedStatus));

    // Mock AspectDao to return streams for both calls (removed properties and all properties)
    when(freshMockAspectDao.streamAspects(anyString(), anyString()))
        .thenReturn(Stream.of(removedAspect)) // First call for removed properties
        .thenReturn(Stream.of(removedAspect)); // Second call for all properties

    Set<Pair<Urn, StructuredPropertyDefinition>> result =
        ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(freshMockAspectDao);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetActiveStructuredPropertiesDefinitionsWithNullStatus() {
    // Create a fresh mock for this test
    AspectDao freshMockAspectDao = mock(AspectDao.class);

    // Mock EntityAspect with null status (should be treated as active)
    EntityAspect aspectWithNullStatus = mock(EntityAspect.class);
    Urn urn = UrnUtils.getUrn("urn:li:structuredProperty:nullstatus");
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setQualifiedName("nullstatus.property");

    when(aspectWithNullStatus.getUrn()).thenReturn(urn.toString());
    when(aspectWithNullStatus.getMetadata()).thenReturn(RecordUtils.toJsonString(definition));

    // Mock AspectDao to return streams for both calls (removed properties and all properties)
    when(freshMockAspectDao.streamAspects(anyString(), anyString()))
        .thenReturn(Stream.empty()) // First call for removed properties (none)
        .thenReturn(Stream.of(aspectWithNullStatus)); // Second call for all properties

    Set<Pair<Urn, StructuredPropertyDefinition>> result =
        ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(freshMockAspectDao);

    assertNotNull(result);
    assertEquals(result.size(), 1);

    // Verify property with null status is returned (treated as active)
    Pair<Urn, StructuredPropertyDefinition> pair = result.iterator().next();
    assertEquals(pair.getFirst(), urn);
    assertEquals(pair.getSecond().getQualifiedName(), definition.getQualifiedName());
  }

  @Test
  public void testGetActiveStructuredPropertiesDefinitionsWithException() {
    // Create a fresh mock for this test
    AspectDao freshMockAspectDao = mock(AspectDao.class);

    // Mock AspectDao to throw exception
    when(freshMockAspectDao.streamAspects(anyString(), anyString()))
        .thenThrow(new RuntimeException("Database error"));

    assertThrows(
        RuntimeException.class,
        () ->
            ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(freshMockAspectDao));
  }

  @Test
  public void testCreateElasticSearchIndexedServicesWithActualInstances() {
    // Create actual mock instances that implement ElasticSearchIndexed
    GraphService graphService =
        mock(GraphService.class, withSettings().extraInterfaces(ElasticSearchIndexed.class));
    EntitySearchService searchService =
        mock(EntitySearchService.class, withSettings().extraInterfaces(ElasticSearchIndexed.class));

    List<ElasticSearchIndexed> result =
        ElasticSearchUpgradeUtils.createElasticSearchIndexedServices(
            graphService, searchService, mockSystemMetadataService, mockTimeseriesAspectService);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains(graphService));
    assertTrue(result.contains(searchService));
  }

  @Test
  public void testGetActiveStructuredPropertiesDefinitionsMultipleActive() {
    // Create a fresh mock for this test
    AspectDao freshMockAspectDao = mock(AspectDao.class);

    // Mock multiple active structured properties
    EntityAspect aspect1 = mock(EntityAspect.class);
    Urn urn1 = UrnUtils.getUrn("urn:li:structuredProperty:prop1");
    StructuredPropertyDefinition def1 = new StructuredPropertyDefinition();
    def1.setQualifiedName("prop1");

    EntityAspect aspect2 = mock(EntityAspect.class);
    Urn urn2 = UrnUtils.getUrn("urn:li:structuredProperty:prop2");
    StructuredPropertyDefinition def2 = new StructuredPropertyDefinition();
    def2.setQualifiedName("prop2");

    Status activeStatus = new Status();
    activeStatus.setRemoved(false);

    when(aspect1.getUrn()).thenReturn(urn1.toString());
    when(aspect1.getMetadata()).thenReturn("{}"); // JSON string representation

    when(aspect2.getUrn()).thenReturn(urn2.toString());
    when(aspect2.getMetadata()).thenReturn("{}"); // JSON string representation

    when(freshMockAspectDao.streamAspects(anyString(), anyString()))
        .thenReturn(Stream.empty()) // First call for removed properties (none)
        .thenReturn(Stream.of(aspect1, aspect2)); // Second call for all properties

    Set<Pair<Urn, StructuredPropertyDefinition>> result =
        ElasticSearchUpgradeUtils.getActiveStructuredPropertiesDefinitions(freshMockAspectDao);

    assertNotNull(result);
    assertEquals(result.size(), 2);

    // Verify both properties are returned
    assertTrue(result.stream().anyMatch(pair -> pair.getFirst().equals(urn1)));
    assertTrue(result.stream().anyMatch(pair -> pair.getFirst().equals(urn2)));
  }
}
