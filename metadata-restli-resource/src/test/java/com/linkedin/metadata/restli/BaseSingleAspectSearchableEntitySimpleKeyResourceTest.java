package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.singleaspectentity.EntityAspectUnion;
import com.linkedin.testing.singleaspectentity.EntityDocument;
import com.linkedin.testing.singleaspectentity.EntitySnapshot;
import com.linkedin.testing.singleaspectentity.EntityValue;
import com.linkedin.testing.urn.SingleAspectEntityUrn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseSingleAspectSearchableEntitySimpleKeyResourceTest extends BaseEngineTest {
  private BaseLocalDAO<EntityAspectUnion, SingleAspectEntityUrn> _mockLocalDao;
  private BaseSearchDAO<EntityDocument> _mockSearchDao;
  private TestResource _resource = new TestResource();

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setup() {
    _mockLocalDao = mock(BaseLocalDAO.class);
    _mockSearchDao = mock(BaseSearchDAO.class);
  }

  @Test
  public void testSearch() throws URISyntaxException {
    long id1 = 100L;
    String field1 = "foo";
    SingleAspectEntityUrn urn1 = new SingleAspectEntityUrn(id1);
    AspectBar aspect1 = new AspectBar().setValue(field1);
    EntityValue value1 = new EntityValue().setValue(field1).setId(id1);
    AspectKey<SingleAspectEntityUrn, AspectBar> key1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);

    String searchInput = "foo";

    Filter filter = QueryUtils.EMPTY_FILTER;
    PagingContext pagingContext = new PagingContext(1, 2);
    EntityDocument document = new EntityDocument().setF1(field1).setUrn(urn1);

    SearchResult<EntityDocument> searchResult = SearchResult.<EntityDocument>builder()
        .documentList(Collections.singletonList(document))
        .searchResultMetadata(mock(SearchResultMetadata.class))
        .build();

    when(_mockSearchDao.search(
        searchInput, filter, null, pagingContext.getStart(), pagingContext.getCount()))
        .thenReturn(searchResult);
    when(_mockLocalDao.get(ImmutableSet.of(key1)))
        .thenReturn(ImmutableMap.of(key1, Optional.of(aspect1)));

    CollectionResult<EntityValue, SearchResultMetadata> result =
        runAndWait(_resource.search(searchInput, new String[0], filter, null, new PagingContext(1, 2)));

    assertEquals(result.getElements().size(), 1);
    assertEquals(result.getElements().get(0), value1);
  }

  @Test
  public void testAutoComplete() {
    String input1 = "foo";
    String fieldName = "f1";

    Filter filter = QueryUtils.EMPTY_FILTER;

    when(_mockSearchDao.autoComplete(input1, fieldName, filter, 100))
        .thenReturn(new AutoCompleteResult()
            .setQuery(input1)
            .setSuggestions(new StringArray(Arrays.asList("foo0", "foo1", "foo2"))));

    AutoCompleteResult result = runAndWait(_resource.autocomplete("foo", fieldName, filter, 100));

    assertEquals(result.getQuery(), input1);
    assertEquals(result.getSuggestions().size(), 3);
    assertEquals(result.getSuggestions().get(0), "foo0");
    assertEquals(result.getSuggestions().get(1), "foo1");
    assertEquals(result.getSuggestions().get(2), "foo2");
  }

  @Test
  public void testGetAll() throws URISyntaxException {
    Filter filter = QueryUtils.EMPTY_FILTER;
    SortCriterion sortCriterion = new SortCriterion();

    long id1 = 100L;
    SingleAspectEntityUrn urn1 = new SingleAspectEntityUrn(id1);
    String field1 = "foo";
    AspectBar aspect1 = new AspectBar().setValue(field1);
    EntityValue value1 = new EntityValue().setValue(field1).setId(id1);
    EntityDocument document1 = new EntityDocument().setF1(field1).setUrn(urn1);
    AspectKey<SingleAspectEntityUrn, AspectBar> key1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);

    long id11 = 200L;
    SingleAspectEntityUrn urn11 = new SingleAspectEntityUrn(id11);
    String field11 = "bar";
    AspectBar aspect11 = new AspectBar().setValue(field11);
    EntityValue value11 = new EntityValue().setValue(field11).setId(id11);
    EntityDocument document11 = new EntityDocument().setF1(field11).setUrn(urn11);
    AspectKey<SingleAspectEntityUrn, AspectBar> key11 = new AspectKey<>(AspectBar.class, urn11, LATEST_VERSION);

    PagingContext pagingContext1 = new PagingContext(0, 2);

    SearchResult<EntityDocument> searchResult1 = SearchResult.<EntityDocument>builder()
        .documentList(Arrays.asList(document1, document11))
        .searchResultMetadata(new SearchResultMetadata())
        .build();

    when(_mockSearchDao.filter(filter, sortCriterion, pagingContext1.getStart(), pagingContext1.getCount()))
        .thenReturn(searchResult1);

    when(_mockLocalDao.get(ImmutableSet.of(key1, key11)))
        .thenReturn(ImmutableMap.of(key1, Optional.of(aspect1), key11, Optional.of(aspect11)));

    List<EntityValue> values = runAndWait(_resource.getAll(pagingContext1, new String[0], filter, sortCriterion));
    assertEquals(values, ImmutableList.of(value1, value11));
  }

  /**
   * Test class for BaseSingleAspectSearchableEntitySimpleKeyResource.
   * */
  private class TestResource extends BaseSingleAspectSearchableEntitySimpleKeyResource<Long, EntityValue,
      SingleAspectEntityUrn, AspectBar, EntityAspectUnion, EntitySnapshot, EntityDocument> {

    TestResource() {
      super(AspectBar.class, EntityAspectUnion.class, EntityValue.class, EntitySnapshot.class);
    }

    @Override
    @Nonnull
    protected BaseSearchDAO<EntityDocument> getSearchDAO() {
      return _mockSearchDao;
    }

    @Override
    @Nonnull
    protected BaseLocalDAO<EntityAspectUnion, SingleAspectEntityUrn> getLocalDAO() {
      return _mockLocalDao;
    }

    @Override
    @Nonnull
    protected SingleAspectEntityUrn createUrnFromString(@Nonnull String urnString) throws Exception {
      return SingleAspectEntityUrn.createFromString(urnString);
    }

    @Override
    @Nonnull
    protected SingleAspectEntityUrn toUrn(@Nonnull Long aLong) {
      try {
        return new SingleAspectEntityUrn(aLong);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    @Nonnull
    protected EntityValue createEntity(@Nonnull EntityValue partialEntity, @Nonnull SingleAspectEntityUrn urn) {
      return partialEntity.setId(urn.getIdAsLong());
    }
  }
}
