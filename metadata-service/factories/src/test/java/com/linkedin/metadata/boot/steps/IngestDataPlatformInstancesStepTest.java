package com.linkedin.metadata.boot.steps;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;


/**
 * Test the behavior of IngestDataPlatformInstancesStep.
 *
 * We expect it to check if any data platform instance aspects already exist in the database and if none are found,
 * to go through all the stored entities and ingest a data platform instance aspect for any that are compatible with it.
 *
 * CorpUser is used as an example of an entity that is not compatible with data platform instance and therefore should be ignored.
 * Char is used as an example of an entity that should get adorned with a data platform instance.
 *
 * See {@link DataPlatformInstanceUtils} for the compatibility rules.
 */
public class IngestDataPlatformInstancesStepTest {

  @Test
  public void testExecuteDoesNothingWhenDataPlatformInstanceAspectsAlreadyExists() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    final AspectMigrationsDao migrationsDao = mock(AspectMigrationsDao.class);

    mockDBWithDataPlatformInstanceAspects(migrationsDao);

    final IngestDataPlatformInstancesStep step = new IngestDataPlatformInstancesStep(entityService, migrationsDao);
    step.execute();

    verify(migrationsDao, times(1)).checkIfAspectExists(anyString());
    verifyNoMoreInteractions(migrationsDao);
    verifyZeroInteractions(entityService);
  }

  @Test
  public void testExecuteCopesWithEmptyDB() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    final AspectMigrationsDao migrationsDao = mock(AspectMigrationsDao.class);

    mockEmptyDB(migrationsDao);

    final IngestDataPlatformInstancesStep step = new IngestDataPlatformInstancesStep(entityService, migrationsDao);
    step.execute();

    verify(migrationsDao, times(1)).checkIfAspectExists(anyString());
    verify(migrationsDao, times(1)).countEntities();
    verifyNoMoreInteractions(migrationsDao);
    verifyZeroInteractions(entityService);
  }

  @Test
  public void testExecuteChecksKeySpecForAllUrns() throws Exception {
    final EntityRegistry entityRegistry = getTestEntityRegistry();
    final EntityService entityService = mock(EntityService.class);
    final AspectMigrationsDao migrationsDao = mock(AspectMigrationsDao.class);
    final int countOfCorpUserEntities = 2;
    final int countOfChartEntities = 4;
    final int totalUrnsInDB = countOfCorpUserEntities + countOfChartEntities;

    mockDBWithWorkToDo(entityRegistry, entityService, migrationsDao, countOfCorpUserEntities, countOfChartEntities);

    final IngestDataPlatformInstancesStep step = new IngestDataPlatformInstancesStep(entityService, migrationsDao);
    step.execute();

    verify(entityService, times(totalUrnsInDB)).getKeyAspectSpec(any(Urn.class));
  }

  @Test
  public void testExecuteWhenSomeEntitiesShouldReceiveDataPlatformInstance() throws Exception {
    final EntityRegistry entityRegistry = getTestEntityRegistry();
    final EntityService entityService = mock(EntityService.class);
    final AspectMigrationsDao migrationsDao = mock(AspectMigrationsDao.class);
    final int countOfCorpUserEntities = 5;
    final int countOfChartEntities = 7;

    mockDBWithWorkToDo(entityRegistry, entityService, migrationsDao, countOfCorpUserEntities, countOfChartEntities);

    final IngestDataPlatformInstancesStep step = new IngestDataPlatformInstancesStep(entityService, migrationsDao);
    step.execute();

    verify(entityService, times(countOfChartEntities))
        .ingestAspect(
            argThat(arg -> arg.getEntityType().equals("chart")),
            eq(DATA_PLATFORM_INSTANCE_ASPECT_NAME),
            any(DataPlatformInstance.class),
            any(),
            any());
    verify(entityService, times(0))
        .ingestAspect(argThat(arg -> !arg.getEntityType().equals("chart")), anyString(), any(), any(), any());
  }

  @NotNull
  private ConfigEntityRegistry getTestEntityRegistry() {
    return new ConfigEntityRegistry(
        IngestDataPlatformInstancesStepTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yaml"));
  }

  private void mockDBWithDataPlatformInstanceAspects(AspectMigrationsDao migrationsDao) {
    when(migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME)).thenReturn(true);
  }

  private void mockEmptyDB(AspectMigrationsDao migrationsDao) {
    when(migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME)).thenReturn(false);
    when(migrationsDao.countEntities()).thenReturn(0L);
  }

  private void mockDBWithWorkToDo(
      EntityRegistry entityRegistry,
      EntityService entityService,
      AspectMigrationsDao migrationsDao,
      int countOfCorpUserEntities,
      int countOfChartEntities) {
    List<Urn> corpUserUrns = insertMockEntities(countOfCorpUserEntities, "corpuser", "urn:li:corpuser:test%d", entityRegistry, entityService);
    List<Urn> charUrns = insertMockEntities(countOfChartEntities, "chart", "urn:li:chart:(looker,test%d)", entityRegistry, entityService);
    List<String> allUrnsInDB = Stream.concat(corpUserUrns.stream(), charUrns.stream()).map(Urn::toString).collect(Collectors.toList());
    when(migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME)).thenReturn(false);
    when(migrationsDao.countEntities()).thenReturn((long) allUrnsInDB.size());
    when(migrationsDao.listAllUrns(anyInt(), anyInt())).thenReturn(allUrnsInDB);
  }

  private List<Urn> insertMockEntities(int count, String entity, String urnTemplate, EntityRegistry entityRegistry, EntityService entityService) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entity);
    AspectSpec keySpec = entitySpec.getKeyAspectSpec();
    List<Urn> urns = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Urn urn = UrnUtils.getUrn(String.format(urnTemplate, i));
      urns.add(urn);
      when(entityService.getKeyAspectSpec(urn)).thenReturn(keySpec);
    }
    return urns;
  }
}
