package com.linkedin.metadata.service;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import org.joda.time.DateTimeUtils;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertThrows;

public class LineageServiceTest {
  private static AuditStamp _auditStamp;
  private static EntityClient _mockClient;
  private LineageService _lineageService;
  private static final Authentication AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, "test"), "");
  private static final String ACTOR_URN = "urn:li:corpuser:test";
  private static final String DATASET_URN_1 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test1,DEV)";
  private static final String DATASET_URN_2 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test2,DEV)";
  private static final String DATASET_URN_3 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test3,DEV)";
  private static final String DATASET_URN_4 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test4,DEV)";
  private static final String CHART_URN_1 = "urn:li:dataset:(looker,baz1)";
  private Urn actorUrn;
  private Urn datasetUrn1;
  private Urn datasetUrn2;
  private Urn datasetUrn3;
  private Urn chartUrn1;

  @BeforeMethod
  public void setupTest() {
    DateTimeUtils.setCurrentMillisFixed(123L);
    _auditStamp = new AuditStamp().setActor(UrnUtils.getUrn(ACTOR_URN)).setTime(123L);
    _mockClient = Mockito.mock(EntityClient.class);
    actorUrn = UrnUtils.getUrn(ACTOR_URN);
    datasetUrn1 = UrnUtils.getUrn(DATASET_URN_1);
    datasetUrn2 = UrnUtils.getUrn(DATASET_URN_2);
    datasetUrn3 = UrnUtils.getUrn(DATASET_URN_3);
    chartUrn1 = UrnUtils.getUrn(CHART_URN_1);

    _lineageService = new LineageService(_mockClient);
  }

  // TODO: Add tests for permissions once we add permissions for updating lineage

  // Adds upstream for dataset1 to dataset2 and removes edge to dataset3
  @Test
  public void testUpdateDatasetLineage() throws Exception {
    Mockito.when(_mockClient.exists(datasetUrn1, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn3, AUTHENTICATION)).thenReturn(true);

    UpstreamLineage upstreamLineage = createUpstreamLineage(new ArrayList<>(Arrays.asList(DATASET_URN_3, DATASET_URN_4)));

    Mockito.when(_mockClient.getV2(
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(datasetUrn1),
            Mockito.eq(ImmutableSet.of(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)),
            Mockito.eq(AUTHENTICATION)
        ))
        .thenReturn(
            new EntityResponse()
                .setUrn(datasetUrn1)
                .setEntityName(Constants.DATASET_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                    Constants.UPSTREAM_LINEAGE_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(upstreamLineage.data()))
                )))
        );

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(datasetUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.singletonList(datasetUrn3);
    _lineageService.updateDatasetLineage(datasetUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION);

    // upstreamLineage without dataset3, keep dataset4, add dataset2
    final UpstreamLineage updatedDataset1UpstreamLineage = createUpstreamLineage(new ArrayList<>(Arrays.asList(DATASET_URN_4, DATASET_URN_2)));
    final MetadataChangeProposal proposal1 = new MetadataChangeProposal();
    proposal1.setEntityUrn(UrnUtils.getUrn(DATASET_URN_1));
    proposal1.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal1.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    proposal1.setAspect(GenericRecordUtils.serializeAspect(updatedDataset1UpstreamLineage));
    proposal1.setChangeType(ChangeType.UPSERT);
    Mockito.verify(_mockClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal1),
        Mockito.eq(AUTHENTICATION),
        Mockito.eq(false)
    );
  }

  @Test
  public void testFailUpdateWithMissingDataset() throws Exception {
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(false);

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(datasetUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.singletonList(datasetUrn3);
    assertThrows(IllegalArgumentException.class, () ->
        _lineageService.updateDatasetLineage(datasetUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  @Test
  public void testFailUpdateDatasetWithInvalidEdge() throws Exception {
    Mockito.when(_mockClient.exists(chartUrn1, AUTHENTICATION)).thenReturn(true);

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(chartUrn1);
    final List<Urn> upstreamUrnsToRemove = Collections.emptyList();
    assertThrows(RuntimeException.class, () ->
        _lineageService.updateDatasetLineage(datasetUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));

  }

  private UpstreamLineage createUpstreamLineage(List<String> upstreamUrns) throws Exception {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();
    for (String upstreamUrn : upstreamUrns) {
      Upstream upstream = new Upstream();
      upstream.setDataset(DatasetUrn.createFromString(upstreamUrn));
      upstream.setAuditStamp(_auditStamp);
      upstream.setCreatedAuditStamp(_auditStamp);
      upstream.setType(DatasetLineageType.TRANSFORMED);
      upstreams.add(upstream);
    }
    upstreamLineage.setUpstreams(upstreams);
    return upstreamLineage;
  }
}
