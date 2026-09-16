package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class ParentDataProductsResolverTest {

  private static final Urn CHILD = UrnUtils.getUrn("urn:li:dataProduct:child");
  private static final Urn PARENT = UrnUtils.getUrn("urn:li:dataProduct:parent");
  private static final Urn ROOT = UrnUtils.getUrn("urn:li:dataProduct:root");

  private static EntityResponse responseWithParent(Urn urn, Urn parentUrn) {
    DataProductProperties props = new DataProductProperties().setName(urn.getId());
    if (parentUrn != null) {
      props.setParentDataProduct(parentUrn);
    }
    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));
    return new EntityResponse()
        .setEntityName(DATA_PRODUCT_ENTITY_NAME)
        .setUrn(urn)
        .setAspects(new EnvelopedAspectMap(aspects));
  }

  private static void stubResolveParent(EntityClient client, Urn urn, Urn parentUrn)
      throws Exception {
    when(client.batchGetV2(
            any(),
            eq(DATA_PRODUCT_ENTITY_NAME),
            eq(Collections.singleton(urn)),
            eq(Collections.singleton(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Collections.singletonMap(urn, responseWithParent(urn, parentUrn)));
  }

  @Test
  public void testEmptyParents() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();
    stubResolveParent(mockClient, CHILD, null);

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
    DataProduct source = new DataProduct();
    source.setUrn(CHILD.toString());
    source.setType(EntityType.DATA_PRODUCT);
    when(mockEnv.getSource()).thenReturn(source);

    ParentDataProductsResolver resolver = new ParentDataProductsResolver(mockClient);
    List<DataProduct> result = resolver.get(mockEnv).get();

    assertTrue(result.isEmpty());
  }

  @Test
  public void testWalksParentChainNearestFirst() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();

    stubResolveParent(mockClient, CHILD, PARENT);
    stubResolveParent(mockClient, PARENT, ROOT);
    stubResolveParent(mockClient, ROOT, null);

    Map<Urn, EntityResponse> hydrate = new HashMap<>();
    hydrate.put(PARENT, responseWithParent(PARENT, ROOT));
    hydrate.put(ROOT, responseWithParent(ROOT, null));
    when(mockClient.batchGetV2(
            any(), eq(DATA_PRODUCT_ENTITY_NAME), eq(Set.of(PARENT, ROOT)), eq(null), eq(false)))
        .thenReturn(hydrate);

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
    DataProduct source = new DataProduct();
    source.setUrn(CHILD.toString());
    source.setType(EntityType.DATA_PRODUCT);
    when(mockEnv.getSource()).thenReturn(source);

    ParentDataProductsResolver resolver = new ParentDataProductsResolver(mockClient);
    List<DataProduct> result = resolver.get(mockEnv).get();

    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getUrn(), PARENT.toString());
    assertEquals(result.get(1).getUrn(), ROOT.toString());
  }

  @Test
  public void testCycleVisitedSetStopsWalk() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();

    // CHILD -> PARENT -> CHILD (cycle)
    stubResolveParent(mockClient, CHILD, PARENT);
    stubResolveParent(mockClient, PARENT, CHILD);

    Map<Urn, EntityResponse> hydrate = new HashMap<>();
    hydrate.put(PARENT, responseWithParent(PARENT, CHILD));
    when(mockClient.batchGetV2(
            any(), eq(DATA_PRODUCT_ENTITY_NAME), eq(Set.of(PARENT)), eq(null), eq(false)))
        .thenReturn(hydrate);

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
    DataProduct source = new DataProduct();
    source.setUrn(CHILD.toString());
    source.setType(EntityType.DATA_PRODUCT);
    when(mockEnv.getSource()).thenReturn(source);

    ParentDataProductsResolver resolver = new ParentDataProductsResolver(mockClient);
    List<DataProduct> result = resolver.get(mockEnv).get();

    // Visited set includes CHILD from the start, so PARENT is returned once and walk stops
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getUrn(), PARENT.toString());
  }
}
