package com.linkedin.metadata.kafka.hook.siblings;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.FabricType;
import com.linkedin.common.Siblings;
import com.linkedin.common.SubTypes;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URISyntaxException;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;


public class SiblingAssociationHookTest {
  private SiblingAssociationHook _siblingAssociationHook;
  RestliEntityClient _mockEntityClient;
  EntitySearchService _mockSearchService;
  Authentication _mockAuthentication;

  @BeforeMethod
  public void setupTest() {
    EntityRegistry registry = new ConfigEntityRegistry(
        SiblingAssociationHookTest.class.getClassLoader().getResourceAsStream("test-entity-registry-siblings.yml"));
    _mockEntityClient = Mockito.mock(RestliEntityClient.class);
    _mockSearchService = Mockito.mock(EntitySearchService.class);
    _mockAuthentication = Mockito.mock(Authentication.class);
    _siblingAssociationHook = new SiblingAssociationHook(registry, _mockEntityClient, _mockSearchService, _mockAuthentication,
        true);
    _siblingAssociationHook.setEnabled(true);
  }

  @Test
  public void testInvokeWhenThereIsAPairWithDbtSourceNode() throws Exception {
    SubTypes mockSourceSubtypesAspect = new SubTypes();
    mockSourceSubtypesAspect.setTypeNames(new StringArray(ImmutableList.of("source")));
    EnvelopedAspectMap mockResponseMap = new EnvelopedAspectMap();
    mockResponseMap.put(SUB_TYPES_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(mockSourceSubtypesAspect.data())));
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setAspects(mockResponseMap);

    Mockito.when(_mockEntityClient.exists(Mockito.any(), Mockito.any())).thenReturn(true);


    Mockito.when(
        _mockEntityClient.getV2(
            DATASET_ENTITY_NAME,
            Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"),
            ImmutableSet.of(SUB_TYPES_ASPECT_NAME),
            _mockAuthentication
        )).thenReturn(mockResponse);


    MetadataChangeLog event = createEvent(DATASET_ENTITY_NAME, UPSTREAM_LINEAGE_ASPECT_NAME, ChangeType.UPSERT);

    Upstream upstream = createUpstream("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)", DatasetLineageType.TRANSFORMED);
    final UpstreamLineage upstreamLineage = new UpstreamLineage();
    final UpstreamArray upstreamArray = new UpstreamArray();

    upstreamArray.add(upstream);
    upstreamLineage.setUpstreams(upstreamArray);

    event.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    _siblingAssociationHook.invoke(event);

    final Siblings dbtSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(true);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    proposal.setEntityType(DATASET_ENTITY_NAME);
    proposal.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dbtSiblingsAspect));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.eq(_mockAuthentication)
    );

    final Siblings sourceSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(false);

    final MetadataChangeProposal proposal2 = new MetadataChangeProposal();
    proposal2.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    proposal2.setEntityType(DATASET_ENTITY_NAME);
    proposal2.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal2.setAspect(GenericRecordUtils.serializeAspect(sourceSiblingsAspect));
    proposal2.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal2),
        Mockito.eq(_mockAuthentication)
    );
  }

  @Test
  public void testInvokeWhenThereIsNoPairWithDbtModel() throws Exception {
    SubTypes mockSourceSubtypesAspect = new SubTypes();
    mockSourceSubtypesAspect.setTypeNames(new StringArray(ImmutableList.of("model")));

    Mockito.when(_mockEntityClient.exists(Mockito.any(), Mockito.any())).thenReturn(true);

    EnvelopedAspectMap mockResponseMap = new EnvelopedAspectMap();
    mockResponseMap.put(SUB_TYPES_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(mockSourceSubtypesAspect.data())));
    EntityResponse mockResponse = new EntityResponse();
    mockResponse.setAspects(mockResponseMap);

    Mockito.when(_mockEntityClient.exists(Mockito.any(), Mockito.any())).thenReturn(true);


    Mockito.when(
        _mockEntityClient.getV2(
            DATASET_ENTITY_NAME,
            Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"),
            ImmutableSet.of(SUB_TYPES_ASPECT_NAME),
            _mockAuthentication
        )).thenReturn(mockResponse);

    MetadataChangeLog event = createEvent(DATASET_ENTITY_NAME, UPSTREAM_LINEAGE_ASPECT_NAME, ChangeType.UPSERT);
    Upstream upstream = createUpstream("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)", DatasetLineageType.TRANSFORMED);

    final UpstreamLineage upstreamLineage = new UpstreamLineage();
    final UpstreamArray upstreamArray = new UpstreamArray();

    upstreamArray.add(upstream);
    upstreamLineage.setUpstreams(upstreamArray);

    event.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    _siblingAssociationHook.invoke(event);

    final Siblings dbtSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(true);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    proposal.setEntityType(DATASET_ENTITY_NAME);
    proposal.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dbtSiblingsAspect));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityClient, Mockito.times(0)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.eq(_mockAuthentication)
    );
  }

  @Test
  public void testInvokeWhenThereIsAPairWithBigqueryDownstreamNode() throws Exception {
    Mockito.when(_mockEntityClient.exists(Mockito.any(), Mockito.any())).thenReturn(true);


    MetadataChangeLog event = createEvent(DATASET_ENTITY_NAME, UPSTREAM_LINEAGE_ASPECT_NAME, ChangeType.UPSERT);
    final UpstreamLineage upstreamLineage = new UpstreamLineage();
    final UpstreamArray upstreamArray = new UpstreamArray();
    Upstream upstream = createUpstream("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)", DatasetLineageType.TRANSFORMED);

    upstreamArray.add(upstream);
    upstreamLineage.setUpstreams(upstreamArray);

    event.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    _siblingAssociationHook.invoke(event);

    final Siblings dbtSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(true);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    proposal.setEntityType(DATASET_ENTITY_NAME);
    proposal.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dbtSiblingsAspect));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.eq(_mockAuthentication)
    );

    final Siblings sourceSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(false);

    final MetadataChangeProposal proposal2 = new MetadataChangeProposal();
    proposal2.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    proposal2.setEntityType(DATASET_ENTITY_NAME);
    proposal2.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal2.setAspect(GenericRecordUtils.serializeAspect(sourceSiblingsAspect));
    proposal2.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal2),
        Mockito.eq(_mockAuthentication)
    );
  }

  @Test
  public void testInvokeWhenThereIsAKeyBeingReingested() throws Exception {
    Mockito.when(_mockEntityClient.exists(Mockito.any(), Mockito.any())).thenReturn(true);

    SearchResult returnSearchResult = new SearchResult();
    SearchEntityArray returnEntityArray = new SearchEntityArray();
    SearchEntity returnArrayValue = new SearchEntity();
    returnArrayValue.setEntity(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)")
    );
    returnEntityArray.add(returnArrayValue);

    returnSearchResult.setEntities(returnEntityArray);

    Mockito.when(
        _mockSearchService.search(
            any(), anyString(), any(), any(), anyInt(), anyInt(), eq(new SearchFlags().setFulltext(false)
                        .setSkipAggregates(true).setSkipHighlighting(true))
        )).thenReturn(returnSearchResult);

    MetadataChangeLog event = createEvent(DATASET_ENTITY_NAME, DATASET_KEY_ASPECT_NAME, ChangeType.UPSERT);
    final DatasetKey datasetKey = new DatasetKey();
    datasetKey.setName("my-proj.jaffle_shop.customers");
    datasetKey.setOrigin(FabricType.PROD);
    datasetKey.setPlatform(DataPlatformUrn.createFromString("urn:li:dataPlatform:bigquery"));

    event.setAspect(GenericRecordUtils.serializeAspect(datasetKey));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    _siblingAssociationHook.invoke(event);

    final Siblings dbtSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(true);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"));
    proposal.setEntityType(DATASET_ENTITY_NAME);
    proposal.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dbtSiblingsAspect));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.eq(_mockAuthentication)
    );

    final Siblings sourceSiblingsAspect = new Siblings()
        .setSiblings(new UrnArray(ImmutableList.of(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.jaffle_shop.customers,PROD)"))))
        .setPrimary(false);

    final MetadataChangeProposal proposal2 = new MetadataChangeProposal();
    proposal2.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    proposal2.setEntityType(DATASET_ENTITY_NAME);
    proposal2.setAspectName(SIBLINGS_ASPECT_NAME);
    proposal2.setAspect(GenericRecordUtils.serializeAspect(sourceSiblingsAspect));
    proposal2.setChangeType(ChangeType.UPSERT);

    Mockito.verify(_mockEntityClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal2),
        Mockito.eq(_mockAuthentication)
    );
  }
  @Test
  public void testInvokeWhenSourceUrnHasTwoDbtUpstreams() throws Exception {

    MetadataChangeLog event = createEvent(DATASET_ENTITY_NAME, UPSTREAM_LINEAGE_ASPECT_NAME, ChangeType.UPSERT);
    final UpstreamLineage upstreamLineage = new UpstreamLineage();
    final UpstreamArray upstreamArray = new UpstreamArray();
    Upstream dbtUpstream1 = createUpstream("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.source_entity1,PROD)", DatasetLineageType.TRANSFORMED);
    Upstream dbtUpstream2 = createUpstream("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.source_entity2,PROD)", DatasetLineageType.TRANSFORMED);
    upstreamArray.add(dbtUpstream1);
    upstreamArray.add(dbtUpstream2);
    upstreamLineage.setUpstreams(upstreamArray);

    event.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    _siblingAssociationHook.invoke(event);


    Mockito.verify(_mockEntityClient, Mockito.times(0)).ingestProposal(
            Mockito.any(),
            Mockito.eq(_mockAuthentication)
    );


  }

  @Test
  public void testInvokeWhenSourceUrnHasTwoUpstreamsOneDbt() throws Exception {

    MetadataChangeLog event = createEvent(DATASET_ENTITY_NAME, UPSTREAM_LINEAGE_ASPECT_NAME, ChangeType.UPSERT);
    final UpstreamLineage upstreamLineage = new UpstreamLineage();
    final UpstreamArray upstreamArray = new UpstreamArray();
    Upstream dbtUpstream = createUpstream("urn:li:dataset:(urn:li:dataPlatform:dbt,my-proj.source_entity1,PROD)", DatasetLineageType.TRANSFORMED);
    Upstream snowflakeUpstream =
            createUpstream("urn:li:dataset:(urn:li:dataPlatform:snowflake,my-proj.jaffle_shop.customers,PROD)", DatasetLineageType.TRANSFORMED);
    upstreamArray.add(dbtUpstream);
    upstreamArray.add(snowflakeUpstream);
    upstreamLineage.setUpstreams(upstreamArray);

    event.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)"));
    _siblingAssociationHook.invoke(event);


    Mockito.verify(_mockEntityClient, Mockito.times(2)).ingestProposal(
            Mockito.any(),
            Mockito.eq(_mockAuthentication)
    );


  }

  private MetadataChangeLog createEvent(String entityType, String aspectName, ChangeType changeType) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(entityType);
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    return event;
  }
  private Upstream createUpstream(String urn, DatasetLineageType upstreamType) {

    final Upstream upstream = new Upstream();
    upstream.setType(upstreamType);
    try {
      upstream.setDataset(DatasetUrn.createFromString(urn));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return upstream;
  }


  }
