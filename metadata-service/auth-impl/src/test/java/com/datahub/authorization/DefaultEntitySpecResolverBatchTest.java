package com.datahub.authorization;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Resolving many resources reads their policy fields in one batch per entity type. */
public class DefaultEntitySpecResolverBatchTest {

  private static final Urn OWNER = UrnUtils.getUrn("urn:li:corpuser:owner");

  private SystemEntityClient entityClient;
  private OperationContext opContext;
  private DefaultEntitySpecResolver resolver;

  @BeforeMethod
  public void setup() throws Exception {
    entityClient = mock(SystemEntityClient.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    resolver = new DefaultEntitySpecResolver(opContext, entityClient, mock(GroupService.class));
    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(DATASET_ENTITY_NAME), anySet(), anySet()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(2);
              return urns.stream().collect(Collectors.toMap(urn -> urn, this::ownedResponse));
            });
  }

  @Test
  public void testResolveAllReadsEachEntityTypeOnce() throws Exception {
    List<EntitySpec> specs =
        IntStream.range(0, 20)
            .mapToObj(
                i ->
                    new EntitySpec(
                        DATASET_ENTITY_NAME,
                        "urn:li:dataset:(urn:li:dataPlatform:hive,table" + i + ",PROD)"))
            .collect(Collectors.toList());

    Map<EntitySpec, ResolvedEntitySpec> resolved = resolver.resolveAll(specs, opContext);

    assertEquals(resolved.keySet(), Set.copyOf(specs));
    for (EntitySpec spec : specs) {
      assertEquals(resolved.get(spec).getOwners(), Set.of(OWNER.toString()));
      assertTrue(resolved.get(spec).getFieldValues(EntityFieldType.TAG).isEmpty());
      assertTrue(resolved.get(spec).getFieldValues(EntityFieldType.CONTAINER).isEmpty());
      assertTrue(
          resolved.get(spec).getFieldValues(EntityFieldType.DATA_PLATFORM_INSTANCE).isEmpty());
    }
    verify(entityClient, times(1))
        .batchGetV2(any(OperationContext.class), eq(DATASET_ENTITY_NAME), anySet(), anySet());
    verify(entityClient, never()).getV2(any(OperationContext.class), anyString(), any(), any());
    verify(entityClient, never()).getV2(any(OperationContext.class), any(Urn.class), any());
  }

  @Test
  public void testDataHubAuthorizerPrefetchSeedsTheResourceSpecCache() {
    ContextualEntitySpecResolver contextualResolver = mock(ContextualEntitySpecResolver.class);
    EntitySpec cached = new EntitySpec(DATASET_ENTITY_NAME, "urn:li:dataset:cached");
    EntitySpec uncached = new EntitySpec(DATASET_ENTITY_NAME, "urn:li:dataset:uncached");
    ResolvedEntitySpec cachedSpec = new ResolvedEntitySpec(cached, Map.of());
    ResolvedEntitySpec uncachedSpec = new ResolvedEntitySpec(uncached, Map.of());
    when(contextualResolver.resolveAll(eq(List.of(uncached)), any()))
        .thenReturn(Map.of(uncached, uncachedSpec));
    DataHubAuthorizer authorizer =
        new DataHubAuthorizer(
            opContext,
            entityClient,
            mock(GroupService.class),
            0,
            0,
            DataHubAuthorizer.AuthorizationMode.DEFAULT,
            1);
    authorizer.init(Map.of(), new AuthorizerContext(Map.of(), contextualResolver));
    Map<EntitySpec, ResolvedEntitySpec> cache = new HashMap<>(Map.of(cached, cachedSpec));

    authorizer.prefetchResourceSpecs(List.of(cached, uncached), cache, opContext);

    assertEquals(cache, Map.of(cached, cachedSpec, uncached, uncachedSpec));
    verify(contextualResolver, never()).resolve(any());
  }

  private EntityResponse ownedResponse(Urn urn) {
    Ownership ownership =
        new Ownership()
            .setOwners(
                new OwnerArray(new Owner().setOwner(OWNER).setType(OwnershipType.TECHNICAL_OWNER)));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(DATASET_ENTITY_NAME)
        .setAspects(
            new EnvelopedAspectMap(
                Map.of(
                    OWNERSHIP_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(OWNERSHIP_ASPECT_NAME)
                        .setValue(new Aspect(ownership.data())))));
  }
}
