package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BackfillBrowsePathsV2StepTest {
  private static final String VERSION = "2";
  private static final String UPGRADE_URN =
      String.format(
          "urn:li:%s:%s",
          Constants.DATA_HUB_UPGRADE_ENTITY_NAME, "backfill-default-browse-paths-v2-step");

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:platform,name,PROD)";
  private static final String DASHBOARD_URN = "urn:li:dashboard:(airflow,id)";
  private static final String CHART_URN = "urn:li:chart:(looker,baz)";
  private static final String DATA_JOB_URN =
      "urn:li:dataJob:(urn:li:dataFlow:(airflow,test,prod),test1)";
  private static final String DATA_FLOW_URN = "urn:li:dataFlow:(orchestrator,flowId,cluster)";
  private static final String ML_MODEL_URN =
      "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)";
  private static final String ML_MODEL_GROUP_URN =
      "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,a-model-package-group,PROD)";
  private static final String ML_FEATURE_TABLE_URN =
      "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,user_features)";
  private static final String ML_FEATURE_URN = "urn:li:mlFeature:(test,feature_1)";
  private static final List<String> ENTITY_TYPES =
      ImmutableList.of(
          Constants.DATASET_ENTITY_NAME,
          Constants.DASHBOARD_ENTITY_NAME,
          Constants.CHART_ENTITY_NAME,
          Constants.DATA_JOB_ENTITY_NAME,
          Constants.DATA_FLOW_ENTITY_NAME,
          Constants.ML_MODEL_ENTITY_NAME,
          Constants.ML_MODEL_GROUP_ENTITY_NAME,
          Constants.ML_FEATURE_TABLE_ENTITY_NAME,
          Constants.ML_FEATURE_ENTITY_NAME);
  private static final List<Urn> ENTITY_URNS =
      ImmutableList.of(
          UrnUtils.getUrn(DATASET_URN),
          UrnUtils.getUrn(DASHBOARD_URN),
          UrnUtils.getUrn(CHART_URN),
          UrnUtils.getUrn(DATA_JOB_URN),
          UrnUtils.getUrn(DATA_FLOW_URN),
          UrnUtils.getUrn(ML_MODEL_URN),
          UrnUtils.getUrn(ML_MODEL_GROUP_URN),
          UrnUtils.getUrn(ML_FEATURE_TABLE_URN),
          UrnUtils.getUrn(ML_FEATURE_URN));

  @Test
  public void testExecuteNoExistingBrowsePaths() throws Exception {
    final EntityService mockService = initMockService();
    final SearchService mockSearchService = initMockSearchService();

    final Urn upgradeEntityUrn = Urn.createFromString(UPGRADE_URN);
    Mockito.when(
            mockService.getEntityV2(
                Mockito.eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
                Mockito.eq(upgradeEntityUrn),
                Mockito.eq(Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME))))
        .thenReturn(null);

    BackfillBrowsePathsV2Step backfillBrowsePathsV2Step =
        new BackfillBrowsePathsV2Step(mockService, mockSearchService);
    backfillBrowsePathsV2Step.execute();

    Mockito.verify(mockSearchService, Mockito.times(9))
        .scrollAcrossEntities(
            Mockito.any(),
            Mockito.eq("*"),
            Mockito.any(Filter.class),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.eq("5m"),
            Mockito.eq(5000),
            Mockito.eq(null));
    // Verify that 11 aspects are ingested, 2 for the upgrade request / result, 9 for ingesting 1 of
    // each entity type
    Mockito.verify(mockService, Mockito.times(11))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testDoesNotRunWhenAlreadyExecuted() throws Exception {
    final EntityService mockService = Mockito.mock(EntityService.class);
    final SearchService mockSearchService = initMockSearchService();

    final Urn upgradeEntityUrn = Urn.createFromString(UPGRADE_URN);
    com.linkedin.upgrade.DataHubUpgradeRequest upgradeRequest =
        new com.linkedin.upgrade.DataHubUpgradeRequest().setVersion(VERSION);
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

    BackfillBrowsePathsV2Step backfillBrowsePathsV2Step =
        new BackfillBrowsePathsV2Step(mockService, mockSearchService);
    backfillBrowsePathsV2Step.execute();

    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  private EntityService initMockService() throws URISyntaxException {
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntityRegistry registry = new UpgradeDefaultBrowsePathsStepTest.TestEntityRegistry();
    Mockito.when(mockService.getEntityRegistry()).thenReturn(registry);

    for (int i = 0; i < ENTITY_TYPES.size(); i++) {
      Mockito.when(
              mockService.buildDefaultBrowsePathV2(
                  Mockito.eq(ENTITY_URNS.get(i)), Mockito.eq(true)))
          .thenReturn(
              new BrowsePathsV2()
                  .setPath(new BrowsePathEntryArray(new BrowsePathEntry().setId("test"))));

      Mockito.when(
              mockService.getEntityV2(
                  Mockito.any(),
                  Mockito.eq(ENTITY_URNS.get(i)),
                  Mockito.eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
          .thenReturn(null);
    }

    return mockService;
  }

  private SearchService initMockSearchService() {
    final SearchService mockSearchService = Mockito.mock(SearchService.class);

    for (int i = 0; i < ENTITY_TYPES.size(); i++) {
      Mockito.when(
              mockSearchService.scrollAcrossEntities(
                  Mockito.eq(ImmutableList.of(ENTITY_TYPES.get(i))),
                  Mockito.eq("*"),
                  Mockito.any(Filter.class),
                  Mockito.eq(null),
                  Mockito.eq(null),
                  Mockito.eq("5m"),
                  Mockito.eq(5000),
                  Mockito.eq(null)))
          .thenReturn(
              new ScrollResult()
                  .setNumEntities(1)
                  .setEntities(
                      new SearchEntityArray(new SearchEntity().setEntity(ENTITY_URNS.get(i)))));
    }

    return mockSearchService;
  }
}
