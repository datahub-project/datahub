package com.linkedin.metadata.boot;

import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.AspectIngestionUtils;
import com.linkedin.metadata.boot.steps.IngestDataPlatformInstancesStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

abstract public class IngestDataPlatformInstancesTest<T_AD extends AspectDao, T_AMD extends AspectMigrationsDao> {

  protected T_AD _aspectDao;
  protected T_AMD _migrationsDao;

  protected final EntityRegistry _snapshotEntityRegistry;
  protected final EntityRegistry _configEntityRegistry;
  protected final EntityRegistry _testEntityRegistry;
  protected EventProducer _mockProducer;

  protected EntityService _entityService;
  protected RetentionService _retentionService;

  protected IngestDataPlatformInstancesStep _step;

  protected IngestDataPlatformInstancesTest() throws EntityRegistryException {
    _snapshotEntityRegistry = new TestEntityRegistry();
    _configEntityRegistry = new ConfigEntityRegistry(Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    _testEntityRegistry = new MergedEntityRegistry(_snapshotEntityRegistry).apply(_configEntityRegistry);
  }

  @Test
  public void testExecute() throws Exception {
    final int corpUserCount = 30;
    final int chartCount = 30;
    // Ingest some CorpUserInfo aspects
    // CorpUser has no data platform associated to it, so it should be ignored
    Map<Urn, CorpUserInfo> corpUsers = AspectIngestionUtils.ingestCorpUserInfoAspects(_entityService, corpUserCount);

    // Ingest loads of ChartInfo aspects to make sure that the step needs to run multiple batches
    // Charts are associated with a data platform, so they should receive a data platform instance aspect
    Map<Urn, ChartInfo> charts = AspectIngestionUtils.ingestChartInfoAspects(_entityService, chartCount);

    // Sanity check
    assertFalse(_migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME));

    _step.execute();

    // Check if the step inserted anything
    assertTrue(_migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME));
    // Check CorpUsers didn't receive a new aspect
    for (Urn urn : corpUsers.keySet()) {
      EntityAspect corpUserAspect = _aspectDao.getLatestAspect(urn.toString(), DATA_PLATFORM_INSTANCE_ASPECT_NAME);
      assertNull(corpUserAspect, urn.toString());
    }
    // Check Charts received a new aspect:
    // - Check if the aspect count is as expected
    List<EntityAspect> chartAspects = charts.keySet().stream()
        .map(urn -> _aspectDao.getLatestAspect(urn.toString(), DATA_PLATFORM_INSTANCE_ASPECT_NAME))
        .collect(Collectors.toList());
    assertEquals(charts.keySet().size(), chartAspects.size(), "We should get as many aspects as we have URNs.");
    // - Check for each Chart
    for (Urn wUrn : charts.keySet()) {
      long matchingUrnCount = chartAspects.stream().filter(rAspect -> wUrn.toString().equals(rAspect == null ? null : rAspect.getUrn())).count();
      assertEquals(matchingUrnCount, 1L, String.format("Each URN should appear exactly once. %s appeared %d times.", wUrn, matchingUrnCount));
    }

    // Check the step won't ingest anything if *some* data platform instance aspects are already present
    // (we know some are there because the test would have already failed at this point)

    // Ingest some more entities that would have received the data platform instance on the previous execution
    Map<Urn, ChartInfo> newCharts = AspectIngestionUtils.ingestChartInfoAspects(_entityService, 10, chartCount + 1);

    _step.execute();

    List<EntityAspect> newChartAspects = newCharts.keySet().stream()
        .map(urn -> _aspectDao.getLatestAspect(urn.toString(), DATA_PLATFORM_INSTANCE_ASPECT_NAME))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    // If none of the new charts received a data platform instance aspect - we can safely assume the step execution did nothing
    assertEquals(newChartAspects.size(), 0);
  }
}
