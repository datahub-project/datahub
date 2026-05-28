package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductUpstream;
import com.linkedin.dataproduct.DataProductUpstreamArray;
import com.linkedin.dataproduct.DataProductUpstreams;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductUpstreamsValidatorTest {

  private static final Urn DP_A = UrnUtils.getUrn("urn:li:dataProduct:a");
  private static final Urn DP_B = UrnUtils.getUrn("urn:li:dataProduct:b");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)");

  private DataProductUpstreamsValidator validator;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    validator = new DataProductUpstreamsValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(DataProductUpstreamsValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    new AspectPluginConfig.EntityAspectName(
                        Constants.DATA_PRODUCT_ENTITY_NAME,
                        Constants.DATA_PRODUCT_UPSTREAMS_ASPECT_NAME)))
            .build());
    retrieverContext = mock(RetrieverContext.class);
  }

  @Test
  public void testValidUpstream() {
    BatchItem item = mockItem(DP_A, lineageWith(DP_B));
    assertEquals(
        validator.validateProposed(Set.of(item), retrieverContext, null).count(),
        0L,
        "Expected validation to pass for a DP -> DP upstream edge");
  }

  @Test
  public void testRejectsSelfEdge() {
    BatchItem item = mockItem(DP_A, lineageWith(DP_A));
    assertEquals(
        validator.validateProposed(Set.of(item), retrieverContext, null).count(),
        1L,
        "Expected validation to reject a DP declaring itself as an upstream");
  }

  @Test
  public void testRejectsNonDataProductUpstream() {
    BatchItem item = mockItem(DP_A, lineageWith(DATASET));
    assertTrue(
        validator.validateProposed(Set.of(item), retrieverContext, null).count() >= 1L,
        "Expected validation to reject a DP -> non-DP upstream urn");
  }

  @Test
  public void testEmptyUpstreamsPasses() {
    DataProductUpstreams aspect = new DataProductUpstreams();
    aspect.setUpstreams(new DataProductUpstreamArray());
    BatchItem item = mockItem(DP_A, aspect);
    assertEquals(
        validator.validateProposed(Set.of(item), retrieverContext, null).count(),
        0L,
        "Expected validation to pass with no upstreams");
  }

  private DataProductUpstreams lineageWith(Urn upstream) {
    DataProductUpstreams aspect = new DataProductUpstreams();
    DataProductUpstreamArray upstreams = new DataProductUpstreamArray();
    DataProductUpstream entry = new DataProductUpstream();
    entry.setDataProduct(upstream);
    upstreams.add(entry);
    aspect.setUpstreams(upstreams);
    return aspect;
  }

  private BatchItem mockItem(Urn urn, DataProductUpstreams aspect) {
    BatchItem item = mock(BatchItem.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getAspectName()).thenReturn(Constants.DATA_PRODUCT_UPSTREAMS_ASPECT_NAME);
    when(item.getAspect(any())).thenReturn(aspect);
    return item;
  }
}
