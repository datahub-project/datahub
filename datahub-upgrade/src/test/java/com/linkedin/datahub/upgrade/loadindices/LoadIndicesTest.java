package com.linkedin.datahub.upgrade.loadindices;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.BuildIndicesStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadIndicesTest {

  @Mock private OperationContext mockOperationContext;
  @Mock private Database mockDatabase;
  @Mock private EntityService<?> mockEntityService;
  @Mock private UpdateIndicesService mockUpdateIndicesService;
  @Mock private LoadIndicesIndexManager mockIndexManager;
  @Mock private SystemMetadataService mockSystemMetadataService;
  @Mock private TimeseriesAspectService mockTimeseriesAspectService;
  @Mock private EntitySearchService mockEntitySearchService;
  @Mock private GraphService mockGraphService;
  @Mock private AspectDao mockAspectDao;

  private LoadIndices loadIndices;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    loadIndices =
        new LoadIndices(
            mockDatabase,
            mockEntityService,
            mockUpdateIndicesService,
            mockIndexManager,
            mockSystemMetadataService,
            mockTimeseriesAspectService,
            mockEntitySearchService,
            mockGraphService,
            mockAspectDao);
  }

  @Test
  public void testLoadIndicesInit() {
    assertNotNull(loadIndices);
    assertEquals("LoadIndices", loadIndices.id());
    assertTrue(loadIndices.steps().size() >= 1);

    // With mocked dependencies, the first step should be BuildIndicesStep
    // and the last step should be LoadIndicesStep
    UpgradeStep firstStep = loadIndices.steps().get(0);
    assertTrue(firstStep instanceof BuildIndicesStep);

    UpgradeStep lastStep = loadIndices.steps().get(loadIndices.steps().size() - 1);
    assertTrue(lastStep instanceof LoadIndicesStep);
  }

  @Test
  public void testLoadIndicesStepId() {
    // Test the LoadIndicesStep (last step)
    UpgradeStep loadIndicesStep = loadIndices.steps().get(loadIndices.steps().size() - 1);
    assertEquals("LoadIndicesStep", loadIndicesStep.id());
    assertEquals(0, loadIndicesStep.retryCount());

    // Also test the first step (BuildIndicesStep if dependencies are provided)
    UpgradeStep firstStep = loadIndices.steps().get(0);
    assertTrue(firstStep instanceof BuildIndicesStep);
    assertEquals(0, firstStep.retryCount());
  }

  @Test
  public void testLoadIndicesCleanupSteps() {
    assertTrue(loadIndices.cleanupSteps().isEmpty());
  }

  @Test
  public void testLoadIndicesWithNullDependencies() {
    // Test constructor with null dependencies (graceful degradation)
    LoadIndices loadIndicesWithoutDeps =
        new LoadIndices(null, null, null, null, null, null, null, null, null);
    assertNotNull(loadIndicesWithoutDeps);
    assertEquals("LoadIndices", loadIndicesWithoutDeps.id());
    // When server or indexManager is null, should return empty steps list
    assertEquals(loadIndicesWithoutDeps.steps().size(), 0);
    assertTrue(loadIndicesWithoutDeps.cleanupSteps().isEmpty());
  }
}
