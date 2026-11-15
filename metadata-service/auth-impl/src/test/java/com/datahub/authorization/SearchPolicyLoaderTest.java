package com.datahub.authorization;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchPolicyLoaderTest {

  private static final Urn P1 = UrnUtils.getUrn("urn:li:dataHubPolicy:p1");
  private static final Urn P2 = UrnUtils.getUrn("urn:li:dataHubPolicy:p2");
  private static final Urn P3 = UrnUtils.getUrn("urn:li:dataHubPolicy:p3");

  private static EntityResponse response(Urn urn, boolean withInfo) {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    if (withInfo) {
      aspects.put(
          DATAHUB_POLICY_INFO_ASPECT_NAME,
          new EnvelopedAspect()
              .setValue(new Aspect(new DataHubPolicyInfo().setDisplayName(urn.getId()).data())));
    }
    return new EntityResponse().setUrn(urn).setEntityName(POLICY_ENTITY_NAME).setAspects(aspects);
  }

  private static EntityResponse removed(Urn urn) {
    EntityResponse response = response(urn, true);
    response
        .getAspects()
        .put(
            STATUS_ASPECT_NAME,
            new EnvelopedAspect().setValue(new Aspect(new Status().setRemoved(true).data())));
    return response;
  }

  @Test
  public void testLoadByUrnsReadsWithoutTheCacheAndSkipsMissingAndRemovedPolicies()
      throws Exception {
    OperationContext context = TestOperationContexts.systemContextNoSearchAuthorization();
    SystemEntityClient client = mock(SystemEntityClient.class);
    when(client.batchGetV2NoCache(
            any(OperationContext.class),
            eq(POLICY_ENTITY_NAME),
            eq(Set.of(P1, P2, P3)),
            eq(Set.of(DATAHUB_POLICY_INFO_ASPECT_NAME, STATUS_ASPECT_NAME))))
        .thenReturn(Map.of(P1, response(P1, true), P2, response(P2, false), P3, removed(P3)));

    Map<Urn, PolicyFetcher.Policy> result =
        new SearchPolicyLoader(context, client, 100).loadByUrns(Set.of(P1, P2, P3));

    assertEquals(result.keySet(), Set.of(P1));
    assertEquals(result.get(P1).getPolicyInfo().getDisplayName(), "p1");
    verify(client, never())
        .batchGetV2(any(OperationContext.class), any(String.class), anySet(), anySet());
  }

  @Test
  public void testLoadAllReadsEveryPage() throws Exception {
    OperationContext context = TestOperationContexts.systemContextNoSearchAuthorization();
    SystemEntityClient client = mock(SystemEntityClient.class);
    when(client.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of(POLICY_ENTITY_NAME)),
            eq(""),
            nullable(Filter.class),
            isNull(),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(
            new ScrollResult()
                .setNumEntities(2)
                .setScrollId("next")
                .setEntities(new SearchEntityArray(List.of(new SearchEntity().setEntity(P1)))));
    when(client.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of(POLICY_ENTITY_NAME)),
            eq(""),
            nullable(Filter.class),
            eq("next"),
            isNull(),
            anyList(),
            anyInt()))
        .thenReturn(
            new ScrollResult()
                .setNumEntities(2)
                .setEntities(new SearchEntityArray(List.of(new SearchEntity().setEntity(P2)))));
    when(client.batchGetV2(any(OperationContext.class), eq(POLICY_ENTITY_NAME), anySet(), anySet()))
        .thenAnswer(
            inv -> {
              Set<Urn> urns = inv.getArgument(2);
              return urns.contains(P1)
                  ? Map.of(P1, response(P1, true))
                  : Map.of(P2, response(P2, true));
            });

    List<PolicyFetcher.Policy> result = new SearchPolicyLoader(context, client, 1).loadAll();

    assertEquals(result.stream().map(PolicyFetcher.Policy::getUrn).toList(), List.of(P1, P2));
  }
}
