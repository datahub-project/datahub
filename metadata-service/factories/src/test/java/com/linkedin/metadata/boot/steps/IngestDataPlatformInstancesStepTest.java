package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import io.datahubproject.metadata.context.EntityRegistryContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

/**
 * Test the behavior of IngestDataPlatformInstancesStep.
 *
 * <p>We expect it to check if any data platform instance aspects already exist in the database and
 * if none are found, to go through all the stored entities and ingest a data platform instance
 * aspect for any that are compatible with it.
 *
 * <p>CorpUser is used as an example of an entity that is not compatible with data platform instance
 * and therefore should be ignored. Char is used as an example of an entity that should get adorned
 * with a data platform instance.
 *
 * <p>See {@link DataPlatformInstanceUtils} for the compatibility rules.
 */
public class IngestDataPlatformInstancesStepTest {

  @Test
  public void testExecuteDoesNothingWhenDataPlatformInstanceAspectsAlreadyExists()
      throws Exception {
    final EntityService<?> entityService = mock(EntityService.class);
    final AspectMigrationsDao migrationsDao = mock(AspectMigrationsDao.class);

    mockDBWithDataPlatformInstanceAspects(migrationsDao);

    final IngestDataPlatformInstancesStep step =
        new IngestDataPlatformInstancesStep(entityService, migrationsDao);
    step.execute(mock(OperationContext.class));

    verify(migrationsDao, times(1)).checkIfAspectExists(anyString());
    verifyNoMoreInteractions(migrationsDao);
    verifyNoInteractions(entityService);
  }

  @Test
  public void testExecuteCopesWithEmptyDB() throws Exception {
    final EntityService<?> entityService = mock(EntityService.class);
    final AspectMigrationsDao migrationsDao = mock(AspectMigrationsDao.class);

    mockEmptyDB(migrationsDao);

    final IngestDataPlatformInstancesStep step =
        new IngestDataPlatformInstancesStep(entityService, migrationsDao);
    step.execute(mock(OperationContext.class));

    verify(migrationsDao, times(1)).checkIfAspectExists(anyString());
    verify(migrationsDao, times(1)).countEntities();
    verifyNoMoreInteractions(migrationsDao);
    verifyNoInteractions(entityService);
  }

  @Test
  public void testExecuteChecksKeySpecForAllUrns() throws Exception {
    final EntityRegistry entityRegistry = getTestEntityRegistry();
    final EntityService<?> entityService = mock(EntityService.class);
    final AspectMigrationsDao migrationsDao = mock(AspectMigrationsDao.class);
    final int countOfCorpUserEntities = 2;
    final int countOfChartEntities = 4;
    final int totalUrnsInDB = countOfCorpUserEntities + countOfChartEntities;

    OperationContext mockOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(entityRegistry);
    EntityRegistryContext spyEntityRegistryContext = spy(mockOpContext.getEntityRegistryContext());
    mockOpContext =
        mockOpContext.toBuilder()
            .entityRegistryContext(spyEntityRegistryContext)
            .build(mockOpContext.getSessionAuthentication(), true);

    mockDBWithWorkToDo(migrationsDao, countOfCorpUserEntities, countOfChartEntities);

    final IngestDataPlatformInstancesStep step =
        new IngestDataPlatformInstancesStep(entityService, migrationsDao);

    step.execute(mockOpContext);

    verify(spyEntityRegistryContext, times(totalUrnsInDB)).getKeyAspectSpec(any(Urn.class));
  }

  @Test
  public void testExecuteWhenSomeEntitiesShouldReceiveDataPlatformInstance() throws Exception {
    final EntityRegistry entityRegistry = getTestEntityRegistry();
    final EntityService<?> entityService = mock(EntityService.class);
    final AspectMigrationsDao migrationsDao = mock(AspectMigrationsDao.class);
    final int countOfCorpUserEntities = 5;
    final int countOfChartEntities = 7;

    OperationContext mockOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(entityRegistry);

    mockDBWithWorkToDo(migrationsDao, countOfCorpUserEntities, countOfChartEntities);

    final IngestDataPlatformInstancesStep step =
        new IngestDataPlatformInstancesStep(entityService, migrationsDao);
    step.execute(mockOpContext);

    verify(entityService, times(1))
        .ingestAspects(
            any(OperationContext.class),
            argThat(
                arg ->
                    arg.getItems().stream()
                        .allMatch(
                            item ->
                                item.getUrn().getEntityType().equals("chart")
                                    && item.getAspectName()
                                        .equals(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                                    && ((ChangeItemImpl) item).getRecordTemplate()
                                        instanceof DataPlatformInstance)),
            anyBoolean(),
            anyBoolean());
    verify(entityService, times(0))
        .ingestAspects(
            any(OperationContext.class),
            argThat(
                arg ->
                    !arg.getItems().stream()
                        .allMatch(
                            item ->
                                item.getUrn().getEntityType().equals("chart")
                                    && item.getAspectName()
                                        .equals(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                                    && ((ChangeItemImpl) item).getRecordTemplate()
                                        instanceof DataPlatformInstance)),
            anyBoolean(),
            anyBoolean());
  }

  @NotNull
  private ConfigEntityRegistry getTestEntityRegistry() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    return new ConfigEntityRegistry(
        IngestDataPlatformInstancesStepTest.class
            .getClassLoader()
            .getResourceAsStream("test-entity-registry.yaml"));
  }

  private void mockDBWithDataPlatformInstanceAspects(AspectMigrationsDao migrationsDao) {
    when(migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME)).thenReturn(true);
  }

  private void mockEmptyDB(AspectMigrationsDao migrationsDao) {
    when(migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME)).thenReturn(false);
    when(migrationsDao.countEntities()).thenReturn(0L);
  }

  private void mockDBWithWorkToDo(
      AspectMigrationsDao migrationsDao, int countOfCorpUserEntities, int countOfChartEntities) {
    List<Urn> corpUserUrns = insertMockEntities(countOfCorpUserEntities, "urn:li:corpuser:test%d");
    List<Urn> charUrns = insertMockEntities(countOfChartEntities, "urn:li:chart:(looker,test%d)");
    List<String> allUrnsInDB =
        Stream.concat(corpUserUrns.stream(), charUrns.stream())
            .map(Urn::toString)
            .collect(Collectors.toList());
    when(migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME)).thenReturn(false);
    when(migrationsDao.countEntities()).thenReturn((long) allUrnsInDB.size());
    when(migrationsDao.listAllUrns(anyInt(), anyInt())).thenReturn(allUrnsInDB);
  }

  private List<Urn> insertMockEntities(int count, String urnTemplate) {

    List<Urn> urns = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Urn urn = UrnUtils.getUrn(String.format(urnTemplate, i));
      urns.add(urn);
    }
    return urns;
  }
}
