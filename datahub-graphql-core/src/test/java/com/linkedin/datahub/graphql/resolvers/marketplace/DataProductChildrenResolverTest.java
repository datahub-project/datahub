package com.linkedin.datahub.graphql.resolvers.marketplace;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductChildrenResolverTest {

  private static final String TEST_DATA_PRODUCT_URN = "urn:li:dataProduct:parent";

  private EntityClient _entityClient;
  private ViewService _viewService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private DataProductChildrenResolver _resolver;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _viewService = Mockito.mock(ViewService.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    final Entity entity = Mockito.mock(Entity.class);
    Mockito.when(entity.getUrn()).thenReturn(TEST_DATA_PRODUCT_URN);
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(entity);

    _resolver = new DataProductChildrenResolver(_entityClient, _viewService);
  }

  @Test
  public void testGetSuccess() throws Exception {
    final ScrollResult mockScrollResult = new ScrollResult();
    mockScrollResult.setScrollId("test-scroll-id");
    mockScrollResult.setPageSize(2);
    mockScrollResult.setNumEntities(2);
    mockScrollResult.setMetadata(new SearchResultMetadata());
    mockScrollResult.setEntities(new SearchEntityArray());

    final ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATA_PRODUCT));
    input.setQuery("*");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(),
                eq(Collections.singletonList(Constants.DATA_PRODUCT_ENTITY_NAME)),
                eq("*"),
                any(),
                eq(null),
                eq("5m"),
                eq(Collections.emptyList()),
                eq(10),
                eq(Collections.emptyList())))
        .thenReturn(mockScrollResult);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final ScrollResults result = _resolver.get(_dataFetchingEnvironment).get();

    assertEquals(result.getNextScrollId(), "test-scroll-id");
    assertEquals(result.getCount(), 2);
    assertEquals(result.getTotal(), 2);
  }

  @Test
  public void testParentDataProductFilterIsInjected() throws Exception {
    final ScrollResult mockScrollResult = new ScrollResult();
    mockScrollResult.setPageSize(0);
    mockScrollResult.setNumEntities(0);
    mockScrollResult.setMetadata(new SearchResultMetadata());
    mockScrollResult.setEntities(new SearchEntityArray());

    final ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setQuery("*");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(mockScrollResult);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    _resolver.get(_dataFetchingEnvironment).get();

    Mockito.verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            any(),
            any(),
            Mockito.argThat(
                filter ->
                    filter != null
                        && filter.getOr().stream()
                            .flatMap(cc -> cc.getAnd().stream())
                            .anyMatch(
                                c ->
                                    DataProductChildrenResolver.PARENT_DATA_PRODUCT_FIELD_NAME
                                            .equals(c.getField())
                                        && c.getValues() != null
                                        && c.getValues().contains(TEST_DATA_PRODUCT_URN))),
            any(),
            any(),
            any(),
            any(),
            any());
  }
}
