package com.linkedin.metadata.boot.steps;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.template.AspectTemplateEngine;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.search.utils.BrowsePathUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpgradeDefaultBrowsePathsStepTest {

  private static final String VERSION_1 = "1";
  private static final String UPGRADE_URN =
      String.format(
          "urn:li:%s:%s",
          Constants.DATA_HUB_UPGRADE_ENTITY_NAME, "upgrade-default-browse-paths-step");

  @Test
  public void testExecuteNoExistingBrowsePaths() throws Exception {

    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntityRegistry registry = new TestEntityRegistry();
    Mockito.when(mockService.getEntityRegistry()).thenReturn(registry);

    final Urn upgradeEntityUrn = Urn.createFromString(UPGRADE_URN);
    Mockito.when(
            mockService.getEntityV2(
                Mockito.eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
                Mockito.eq(upgradeEntityUrn),
                Mockito.eq(Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME))))
        .thenReturn(null);

    final List<RecordTemplate> browsePaths1 = Collections.emptyList();

    Mockito.when(
            mockService.listLatestAspects(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(Constants.BROWSE_PATHS_ASPECT_NAME),
                Mockito.eq(0),
                Mockito.eq(5000)))
        .thenReturn(
            new ListResult<>(
                browsePaths1,
                new ListResultMetadata().setExtraInfos(new ExtraInfoArray(Collections.emptyList())),
                0,
                false,
                0,
                0,
                2));
    initMockServiceOtherEntities(mockService);

    UpgradeDefaultBrowsePathsStep upgradeDefaultBrowsePathsStep =
        new UpgradeDefaultBrowsePathsStep(mockService);
    upgradeDefaultBrowsePathsStep.execute();

    Mockito.verify(mockService, Mockito.times(1))
        .listLatestAspects(
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.BROWSE_PATHS_ASPECT_NAME),
            Mockito.eq(0),
            Mockito.eq(5000));
    // Verify that 4 aspects are ingested, 2 for the upgrade request / result, but none for
    // ingesting
    Mockito.verify(mockService, Mockito.times(2))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testExecuteFirstTime() throws Exception {

    Urn testUrn1 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset1,PROD)");
    Urn testUrn2 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset2,PROD)");

    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntityRegistry registry = new TestEntityRegistry();
    Mockito.when(mockService.getEntityRegistry()).thenReturn(registry);
    Mockito.when(mockService.buildDefaultBrowsePath(Mockito.eq(testUrn1)))
        .thenReturn(new BrowsePaths().setPaths(new StringArray(ImmutableList.of("/prod/kafka"))));
    Mockito.when(mockService.buildDefaultBrowsePath(Mockito.eq(testUrn2)))
        .thenReturn(new BrowsePaths().setPaths(new StringArray(ImmutableList.of("/prod/kafka"))));

    final Urn upgradeEntityUrn = Urn.createFromString(UPGRADE_URN);
    Mockito.when(
            mockService.getEntityV2(
                Mockito.eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
                Mockito.eq(upgradeEntityUrn),
                Mockito.eq(Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME))))
        .thenReturn(null);
    final List<RecordTemplate> browsePaths1 =
        ImmutableList.of(
            new BrowsePaths()
                .setPaths(
                    new StringArray(
                        ImmutableList.of(
                            BrowsePathUtils.getLegacyDefaultBrowsePath(testUrn1, registry)))),
            new BrowsePaths()
                .setPaths(
                    new StringArray(
                        ImmutableList.of(
                            BrowsePathUtils.getLegacyDefaultBrowsePath(testUrn2, registry)))));

    final List<ExtraInfo> extraInfos1 =
        ImmutableList.of(
            new ExtraInfo()
                .setUrn(testUrn1)
                .setVersion(0L)
                .setAudit(
                    new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:test")).setTime(0L)),
            new ExtraInfo()
                .setUrn(testUrn2)
                .setVersion(0L)
                .setAudit(
                    new AuditStamp()
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                        .setTime(0L)));

    Mockito.when(
            mockService.listLatestAspects(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(Constants.BROWSE_PATHS_ASPECT_NAME),
                Mockito.eq(0),
                Mockito.eq(5000)))
        .thenReturn(
            new ListResult<>(
                browsePaths1,
                new ListResultMetadata().setExtraInfos(new ExtraInfoArray(extraInfos1)),
                2,
                false,
                2,
                2,
                2));
    initMockServiceOtherEntities(mockService);

    UpgradeDefaultBrowsePathsStep upgradeDefaultBrowsePathsStep =
        new UpgradeDefaultBrowsePathsStep(mockService);
    upgradeDefaultBrowsePathsStep.execute();

    Mockito.verify(mockService, Mockito.times(1))
        .listLatestAspects(
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.BROWSE_PATHS_ASPECT_NAME),
            Mockito.eq(0),
            Mockito.eq(5000));
    // Verify that 4 aspects are ingested, 2 for the upgrade request / result and 2 for the browse
    // pahts
    Mockito.verify(mockService, Mockito.times(4))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testDoesNotRunWhenBrowsePathIsNotQualified() throws Exception {
    // Test for browse paths that are not ingested
    Urn testUrn3 =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset3,PROD)"); // Do not
    // migrate
    Urn testUrn4 =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset4,PROD)"); // Do not
    // migrate

    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntityRegistry registry = new TestEntityRegistry();
    Mockito.when(mockService.getEntityRegistry()).thenReturn(registry);

    final Urn upgradeEntityUrn = Urn.createFromString(UPGRADE_URN);
    Mockito.when(
            mockService.getEntityV2(
                Mockito.eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
                Mockito.eq(upgradeEntityUrn),
                Mockito.eq(Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME))))
        .thenReturn(null);

    final List<RecordTemplate> browsePaths2 =
        ImmutableList.of(
            new BrowsePaths()
                .setPaths(
                    new StringArray(
                        ImmutableList.of(
                            BrowsePathUtils.getDefaultBrowsePath(testUrn3, registry, '.')))),
            new BrowsePaths()
                .setPaths(
                    new StringArray(
                        ImmutableList.of(
                            BrowsePathUtils.getLegacyDefaultBrowsePath(testUrn4, registry),
                            BrowsePathUtils.getDefaultBrowsePath(testUrn4, registry, '.')))));

    final List<ExtraInfo> extraInfos2 =
        ImmutableList.of(
            new ExtraInfo()
                .setUrn(testUrn3)
                .setVersion(0L)
                .setAudit(
                    new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:test")).setTime(0L)),
            new ExtraInfo()
                .setUrn(testUrn4)
                .setVersion(0L)
                .setAudit(
                    new AuditStamp()
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                        .setTime(0L)));

    Mockito.when(
            mockService.listLatestAspects(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(Constants.BROWSE_PATHS_ASPECT_NAME),
                Mockito.eq(0),
                Mockito.eq(5000)))
        .thenReturn(
            new ListResult<>(
                browsePaths2,
                new ListResultMetadata().setExtraInfos(new ExtraInfoArray(extraInfos2)),
                2,
                false,
                2,
                2,
                2));
    initMockServiceOtherEntities(mockService);

    UpgradeDefaultBrowsePathsStep upgradeDefaultBrowsePathsStep =
        new UpgradeDefaultBrowsePathsStep(mockService);
    upgradeDefaultBrowsePathsStep.execute();

    Mockito.verify(mockService, Mockito.times(1))
        .listLatestAspects(
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.BROWSE_PATHS_ASPECT_NAME),
            Mockito.eq(0),
            Mockito.eq(5000));
    // Verify that 2 aspects are ingested, only those for the upgrade step
    Mockito.verify(mockService, Mockito.times(2))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testDoesNotRunWhenAlreadyExecuted() throws Exception {
    final EntityService mockService = Mockito.mock(EntityService.class);

    final Urn upgradeEntityUrn = Urn.createFromString(UPGRADE_URN);
    com.linkedin.upgrade.DataHubUpgradeRequest upgradeRequest =
        new com.linkedin.upgrade.DataHubUpgradeRequest().setVersion(VERSION_1);
    Map<String, EnvelopedAspect> upgradeRequestAspects = new HashMap<>();
    upgradeRequestAspects.put(
        Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data())));
    EntityResponse response =
        new EntityResponse().setAspects(new EnvelopedAspectMap(upgradeRequestAspects));
    Mockito.when(
            mockService.getEntityV2(
                Mockito.eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
                Mockito.eq(upgradeEntityUrn),
                Mockito.eq(Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME))))
        .thenReturn(response);

    UpgradeDefaultBrowsePathsStep step = new UpgradeDefaultBrowsePathsStep(mockService);
    step.execute();

    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  private void initMockServiceOtherEntities(EntityService mockService) {
    List<String> skippedEntityTypes =
        ImmutableList.of(
            Constants.DASHBOARD_ENTITY_NAME,
            Constants.CHART_ENTITY_NAME,
            Constants.DATA_FLOW_ENTITY_NAME,
            Constants.DATA_JOB_ENTITY_NAME);
    for (String entityType : skippedEntityTypes) {
      Mockito.when(
              mockService.listLatestAspects(
                  Mockito.eq(entityType),
                  Mockito.eq(Constants.BROWSE_PATHS_ASPECT_NAME),
                  Mockito.eq(0),
                  Mockito.eq(5000)))
          .thenReturn(
              new ListResult<>(
                  Collections.emptyList(),
                  new ListResultMetadata()
                      .setExtraInfos(new ExtraInfoArray(Collections.emptyList())),
                  0,
                  false,
                  0,
                  0,
                  0));
    }
  }

  public static class TestEntityRegistry implements EntityRegistry {

    private final Map<String, EntitySpec> entityNameToSpec;

    public TestEntityRegistry() {
      entityNameToSpec =
          new EntitySpecBuilder(EntitySpecBuilder.AnnotationExtractionMode.IGNORE_ASPECT_FIELDS)
              .buildEntitySpecs(new Snapshot().schema()).stream()
                  .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
    }

    @Nonnull
    @Override
    public EntitySpec getEntitySpec(@Nonnull final String entityName) {
      String lowercaseEntityName = entityName.toLowerCase();
      if (!entityNameToSpec.containsKey(lowercaseEntityName)) {
        throw new IllegalArgumentException(
            String.format("Failed to find entity with name %s in EntityRegistry", entityName));
      }
      return entityNameToSpec.get(lowercaseEntityName);
    }

    @Nullable
    @Override
    public EventSpec getEventSpec(@Nonnull String eventName) {
      return null;
    }

    @Nonnull
    @Override
    public Map<String, EntitySpec> getEntitySpecs() {
      return entityNameToSpec;
    }

    @NotNull
    @Override
    public Map<String, AspectSpec> getAspectSpecs() {
      return new HashMap<>();
    }

    @Nonnull
    @Override
    public Map<String, EventSpec> getEventSpecs() {
      return Collections.emptyMap();
    }

    @NotNull
    @Override
    public AspectTemplateEngine getAspectTemplateEngine() {
      return new AspectTemplateEngine();
    }
  }
}
