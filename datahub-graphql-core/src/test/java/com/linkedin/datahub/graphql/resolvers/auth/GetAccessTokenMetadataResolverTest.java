package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.token.StatefulTokenService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.access.token.DataHubAccessTokenInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import org.mockito.InOrder;
import org.testng.annotations.Test;

public class GetAccessTokenMetadataResolverTest {

  private static final String TOKEN = "raw-token";
  private static final String TOKEN_HASH = "hashed-token";
  private static final String TOKEN_URN = "urn:li:dataHubAccessToken:hashed-token";

  @Test
  public void testDirectBatchLoadMergesFetchAllBeforeLoad() throws Exception {
    StatefulTokenService tokenService = mock(StatefulTokenService.class);
    when(tokenService.hash(TOKEN)).thenReturn(TOKEN_HASH);
    when(tokenService.tokenUrnFromKey(TOKEN_HASH)).thenReturn(UrnUtils.getUrn(TOKEN_URN));

    Urn tokenUrn = UrnUtils.getUrn(TOKEN_URN);
    Urn actor = UrnUtils.getUrn("urn:li:corpuser:actor");
    Urn owner = UrnUtils.getUrn("urn:li:corpuser:owner");
    DataHubAccessTokenInfo info =
        new DataHubAccessTokenInfo()
            .setName("token")
            .setDescription("desc")
            .setActorUrn(actor)
            .setOwnerUrn(owner)
            .setCreatedAt(1L)
            .setExpiresAt(2L);

    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.batchGetV2(any(), eq(Constants.ACCESS_TOKEN_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                tokenUrn,
                new EntityResponse()
                    .setEntityName(Constants.ACCESS_TOKEN_ENTITY_NAME)
                    .setUrn(tokenUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.ACCESS_TOKEN_INFO_NAME,
                                new EnvelopedAspect().setValue(new Aspect(info.data())))))));

    // Mutable aspect-load map so ensureFetchAll + getOptimizedAspects interact realistically.
    Map<String, AspectLoadContext> loadContexts = new HashMap<>();
    loadContexts.put(
        "AccessTokenMetadata", AspectLoadContext.of(ImmutableSet.of("dataHubAccessTokenKey")));
    QueryContext context = getMockAllowContext();
    doAnswer(
            inv -> {
              String type = inv.getArgument(0);
              AspectLoadContext contributed = inv.getArgument(1);
              loadContexts.merge(type, contributed, AspectLoadContext::union);
              return null;
            })
        .when(context)
        .mergeAspectLoadContext(any(), any());
    when(context.getAspectLoadContext(any()))
        .thenAnswer(inv -> loadContexts.get(inv.getArgument(0)));

    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getArgument("token")).thenReturn(TOKEN);

    GetAccessTokenMetadataResolver resolver =
        new GetAccessTokenMetadataResolver(tokenService, entityClient);
    AccessTokenMetadata result = resolver.get(env).get();

    assertNotNull(result);
    assertEquals(result.getDescription(), "desc");
    assertEquals(result.getActorUrn(), actor.toString());
    assertEquals(result.getOwnerUrn(), owner.toString());
    assertTrue(loadContexts.get("AccessTokenMetadata").isFetchAll());

    InOrder inOrder = inOrder(context, entityClient);
    inOrder
        .verify(context)
        .mergeAspectLoadContext(eq("AccessTokenMetadata"), eq(AspectLoadContext.fetchAll()));
    inOrder
        .verify(entityClient)
        .batchGetV2(
            any(),
            eq(Constants.ACCESS_TOKEN_ENTITY_NAME),
            eq(ImmutableSet.of(tokenUrn)),
            eq(ImmutableSet.of(Constants.ACCESS_TOKEN_INFO_NAME)));
  }
}
