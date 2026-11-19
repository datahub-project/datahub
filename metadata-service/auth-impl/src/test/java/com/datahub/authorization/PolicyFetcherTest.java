package com.datahub.authorization;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class PolicyFetcherTest {

  private static final Urn P1 = UrnUtils.getUrn("urn:li:dataHubPolicy:p1");

  @Test
  public void testStartBasedFetchReadsPastAPageWithOnlyDeletedHits() throws Exception {
    OperationContext context = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityClient client = mock(EntityClient.class);
    when(client.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of(POLICY_ENTITY_NAME)),
            anyString(),
            nullable(Filter.class),
            isNull(),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(
            new ScrollResult()
                .setNumEntities(2)
                .setScrollId("next")
                .setEntities(new SearchEntityArray()));
    when(client.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of(POLICY_ENTITY_NAME)),
            anyString(),
            nullable(Filter.class),
            eq("next"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(
            new ScrollResult()
                .setNumEntities(2)
                .setEntities(new SearchEntityArray(List.of(new SearchEntity().setEntity(P1)))));
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        DATAHUB_POLICY_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new DataHubPolicyInfo().setDisplayName("p1").data())));
    when(client.batchGetV2(any(OperationContext.class), eq(POLICY_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(
            Map.of(
                P1,
                new EntityResponse()
                    .setUrn(P1)
                    .setEntityName(POLICY_ENTITY_NAME)
                    .setAspects(aspects)));

    PolicyFetcher.PolicyFetchResult result =
        new PolicyFetcher(client).fetchPolicies(context, 0, "", 1, null).get();

    assertEquals(result.getPolicies().size(), 1);
    assertEquals(result.getPolicies().get(0).getUrn(), P1);
  }

  @Test
  public void testStartBasedFetchPastTheEndReturnsEmpty() throws Exception {
    OperationContext context = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityClient client = mock(EntityClient.class);
    when(client.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of(POLICY_ENTITY_NAME)),
            anyString(),
            nullable(Filter.class),
            isNull(),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(new ScrollResult().setNumEntities(0).setEntities(new SearchEntityArray()));

    PolicyFetcher.PolicyFetchResult result =
        new PolicyFetcher(client).fetchPolicies(context, 5, "", 1, null).get();

    assertEquals(result, PolicyFetcher.PolicyFetchResult.EMPTY);
  }

  @Test
  public void testPageThatReturnsTheSameScrollIdEndsTheScroll() throws Exception {
    OperationContext context = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityClient client = mock(EntityClient.class);
    when(client.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of(POLICY_ENTITY_NAME)),
            anyString(),
            nullable(Filter.class),
            eq("same"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(
            new ScrollResult()
                .setNumEntities(1)
                .setScrollId("same")
                .setEntities(new SearchEntityArray()));

    PolicyFetcher.PolicyFetchResult result =
        new PolicyFetcher(client).fetchPolicies(context, 1, "same", null);

    assertNull(result.getScrollId());
  }
}
