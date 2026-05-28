package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainUpstream;
import com.linkedin.domain.DomainUpstreamArray;
import com.linkedin.domain.DomainUpstreams;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainUpstreamsValidatorTest {

  private static final Urn DOMAIN_A = UrnUtils.getUrn("urn:li:domain:a");
  private static final Urn DOMAIN_B = UrnUtils.getUrn("urn:li:domain:b");
  private static final Urn DATA_PRODUCT = UrnUtils.getUrn("urn:li:dataProduct:foo");

  private DomainUpstreamsValidator validator;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    validator = new DomainUpstreamsValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(DomainUpstreamsValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    new AspectPluginConfig.EntityAspectName(
                        Constants.DOMAIN_ENTITY_NAME, Constants.DOMAIN_UPSTREAMS_ASPECT_NAME)))
            .build());
    retrieverContext = mock(RetrieverContext.class);
  }

  @Test
  public void testValidUpstream() {
    BatchItem item = mockItem(DOMAIN_A, lineageWith(DOMAIN_B));
    assertEquals(
        validator.validateProposed(Set.of(item), retrieverContext, null).count(),
        0L,
        "Expected validation to pass for a Domain -> Domain upstream edge");
  }

  @Test
  public void testRejectsSelfEdge() {
    BatchItem item = mockItem(DOMAIN_A, lineageWith(DOMAIN_A));
    assertEquals(
        validator.validateProposed(Set.of(item), retrieverContext, null).count(),
        1L,
        "Expected validation to reject a Domain declaring itself as an upstream");
  }

  @Test
  public void testRejectsNonDomainUpstream() {
    BatchItem item = mockItem(DOMAIN_A, lineageWith(DATA_PRODUCT));
    assertTrue(
        validator.validateProposed(Set.of(item), retrieverContext, null).count() >= 1L,
        "Expected validation to reject a Domain -> non-Domain upstream urn");
  }

  @Test
  public void testEmptyUpstreamsPasses() {
    DomainUpstreams aspect = new DomainUpstreams();
    aspect.setUpstreams(new DomainUpstreamArray());
    BatchItem item = mockItem(DOMAIN_A, aspect);
    assertEquals(
        validator.validateProposed(Set.of(item), retrieverContext, null).count(),
        0L,
        "Expected validation to pass with no upstreams");
  }

  private DomainUpstreams lineageWith(Urn upstream) {
    DomainUpstreams aspect = new DomainUpstreams();
    DomainUpstreamArray upstreams = new DomainUpstreamArray();
    DomainUpstream entry = new DomainUpstream();
    entry.setDomain(upstream);
    upstreams.add(entry);
    aspect.setUpstreams(upstreams);
    return aspect;
  }

  private BatchItem mockItem(Urn urn, DomainUpstreams aspect) {
    BatchItem item = mock(BatchItem.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getAspectName()).thenReturn(Constants.DOMAIN_UPSTREAMS_ASPECT_NAME);
    when(item.getAspect(any())).thenReturn(aspect);
    return item;
  }
}
