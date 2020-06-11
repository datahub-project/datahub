package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AggregationMetadata;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.EntityDocument;
import com.linkedin.testing.EntityKey;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.EntityValue;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseSearchableEntityResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, Urn> _mockLocalDAO;
  private BaseSearchDAO<EntityDocument> _mockSearchDAO;
  private TestResource _resource = new TestResource();

  class TestResource extends BaseSearchableEntityResource<
      // format
      EntityKey, EntityValue, Urn, EntitySnapshot, EntityAspectUnion, EntityDocument> {

    public TestResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<EntityAspectUnion, Urn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Nonnull
    @Override
    protected BaseSearchDAO getSearchDAO() {
      return _mockSearchDAO;
    }

    @Nonnull
    @Override
    protected Urn createUrnFromString(@Nonnull String urnString) {
      try {
        return Urn.createFromString(urnString);
      } catch (URISyntaxException e) {
        throw RestliUtils.badRequestException("Invalid URN: " + urnString);
      }
    }

    @Nonnull
    @Override
    protected Urn toUrn(@Nonnull EntityKey key) {
      return makeUrn(key.getId());
    }

    @Nonnull
    @Override
    protected EntityKey toKey(@Nonnull Urn urn) {
      return new EntityKey().setId(urn.getIdAsLong());
    }

    @Nonnull
    @Override
    protected EntityValue toValue(@Nonnull EntitySnapshot snapshot) {
      EntityValue value = new EntityValue();
      ModelUtils.getAspectsFromSnapshot(snapshot).forEach(a -> {
        if (a instanceof AspectFoo) {
          value.setFoo(AspectFoo.class.cast(a));
        } else if (a instanceof AspectBar) {
          value.setBar(AspectBar.class.cast(a));
        }
      });
      return value;
    }

    @Nonnull
    @Override
    protected EntitySnapshot toSnapshot(@Nonnull EntityValue value, @Nonnull Urn urn) {
      EntitySnapshot snapshot = new EntitySnapshot().setUrn(urn);
      EntityAspectUnionArray aspects = new EntityAspectUnionArray();
      if (value.hasFoo()) {
        aspects.add(ModelUtils.newAspectUnion(EntityAspectUnion.class, value.getFoo()));
      }
      if (value.hasBar()) {
        aspects.add(ModelUtils.newAspectUnion(EntityAspectUnion.class, value.getBar()));
      }
      snapshot.setAspects(aspects);
      return snapshot;
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
    _mockSearchDAO = mock(BaseSearchDAO.class);
  }

  @Test
  public void testSearch() {
    Urn urn1 = makeUrn(1);
    Urn urn2 = makeUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspectKey1 = new AspectKey<>(AspectFoo.class, urn1, BaseLocalDAO.LATEST_VERSION);
    AspectKey<Urn, AspectFoo> aspectKey2 = new AspectKey<>(AspectFoo.class, urn2, BaseLocalDAO.LATEST_VERSION);
    SearchResultMetadata searchResultMetadata = makeSearchResultMetadata(new AggregationMetadata().setName("agg")
        .setAggregations(new LongMap(ImmutableMap.of("bucket1", 1L, "bucket2", 2L))));

    when(_mockSearchDAO.search("bar", EMPTY_FILTER, null, 1, 2)).thenReturn(
        makeSearchResult(ImmutableList.of(makeDocument(urn1), makeDocument(urn2)), 10, searchResultMetadata));

    String[] aspectNames = new String[]{ModelUtils.getAspectName(AspectFoo.class)};
    when(_mockLocalDAO.get(ImmutableSet.of(aspectKey1, aspectKey2))).thenReturn(
        ImmutableMap.of(aspectKey1, Optional.of(foo), aspectKey2, Optional.empty()));

    CollectionResult<EntityValue, SearchResultMetadata> searchResult =
        runAndWait(_resource.search("bar", aspectNames, EMPTY_FILTER, null, new PagingContext(1, 2)));

    List<EntityValue> values = searchResult.getElements();
    assertEquals(values.size(), 1);
    assertEquals(values.get(0).getFoo(), foo);
    assertFalse(values.get(0).hasBar());

    assertEquals(searchResult.getTotal().intValue(), 10);
    assertEquals(searchResult.getMetadata(), searchResultMetadata);
  }

  private SearchResult<EntityDocument> makeSearchResult(List<EntityDocument> documents, int totalCount,
      SearchResultMetadata searchResultMetadata) {
    return SearchResult.<EntityDocument>builder().documentList(documents)
        .searchResultMetadata(searchResultMetadata)
        .totalCount(totalCount)
        .build();
  }

  private SearchResultMetadata makeSearchResultMetadata(AggregationMetadata... aggregationMetadata) {
    return new SearchResultMetadata().setSearchResultMetadatas(
        new AggregationMetadataArray(Arrays.asList(aggregationMetadata)));
  }

  @Test
  public void testAutocomplete() {
    when(_mockSearchDAO.autoComplete("foo", "name", EMPTY_FILTER, 100)).thenReturn(
        makeAutoCompleteResult("foo", ImmutableList.of("foo0", "foo1", "foo2")));

    AutoCompleteResult result = runAndWait(_resource.autocomplete("foo", "name", EMPTY_FILTER, 100));

    assertEquals(result.getQuery(), "foo");
    assertEquals(result.getSuggestions().size(), 3);
    assertEquals(result.getSuggestions().get(0), "foo0");
    assertEquals(result.getSuggestions().get(1), "foo1");
    assertEquals(result.getSuggestions().get(2), "foo2");
  }

  @Test
  public void testGetAll() {
    Urn urn1 = makeUrn(1);
    Urn urn2 = makeUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspectKey1 = new AspectKey<>(AspectFoo.class, urn1, BaseLocalDAO.LATEST_VERSION);
    AspectKey<Urn, AspectFoo> aspectKey2 = new AspectKey<>(AspectFoo.class, urn2, BaseLocalDAO.LATEST_VERSION);

    SortCriterion sortCriterion1 = new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING);

    when(_mockSearchDAO.filter(EMPTY_FILTER, sortCriterion1, 1, 2)).thenReturn(
        makeSearchResult(ImmutableList.of(makeDocument(urn1), makeDocument(urn2)), 2, new SearchResultMetadata()));

    String[] aspectNames = new String[]{ModelUtils.getAspectName(AspectFoo.class)};
    when(_mockLocalDAO.get(ImmutableSet.of(aspectKey1, aspectKey2))).thenReturn(
        ImmutableMap.of(aspectKey1, Optional.of(foo), aspectKey2, Optional.of(foo)));

    // test with null filter and null sort criterion
    List<EntityValue> values =
        runAndWait(_resource.getAll(new PagingContext(1, 2), aspectNames, null, null));
    assertEquals(values.size(), 2);
    assertEquals(values.get(0).getFoo(), foo);
    assertFalse(values.get(0).hasBar());
    assertEquals(values.get(1).getFoo(), foo);
    assertFalse(values.get(1).hasBar());

    // test with filter that contains removed = true, with non-null sort criterion
    Filter filter2 = newFilter("removed", "true");
    SortCriterion sortCriterion2 = new SortCriterion().setField("urn").setOrder(SortOrder.DESCENDING);
    when(_mockSearchDAO.filter(filter2, sortCriterion2, 1, 2)).thenReturn(
        makeSearchResult(ImmutableList.of(makeDocument(urn1), makeDocument(urn2)), 2, new SearchResultMetadata()));
    values =
        runAndWait(_resource.getAll(new PagingContext(1, 2), aspectNames, filter2, sortCriterion2));
    assertEquals(values.size(), 2);
    assertEquals(values.get(0).getFoo(), foo);
    assertFalse(values.get(0).hasBar());
    assertEquals(values.get(1).getFoo(), foo);
    assertFalse(values.get(1).hasBar());

    // test the case when there is more results in the search index
    Urn urn3 = makeUrn(3);
    AspectKey<Urn, AspectFoo> aspectKey3 = new AspectKey<>(AspectFoo.class, urn3, BaseLocalDAO.LATEST_VERSION);
    when(_mockSearchDAO.filter(EMPTY_FILTER, sortCriterion1, 1, 3)).thenReturn(
        makeSearchResult(ImmutableList.of(makeDocument(urn1), makeDocument(urn2), makeDocument(urn3)), 3, new SearchResultMetadata()));
    when(_mockLocalDAO.get(ImmutableSet.of(aspectKey1, aspectKey2, aspectKey3))).thenReturn(
        ImmutableMap.of(aspectKey1, Optional.of(foo), aspectKey2, Optional.empty()));
    values =
        runAndWait(_resource.getAll(new PagingContext(1, 3), aspectNames, EMPTY_FILTER, sortCriterion1));
    assertEquals(values.size(), 1);
    assertEquals(values.get(0).getFoo(), foo);
    assertFalse(values.get(0).hasBar());
  }

  private AutoCompleteResult makeAutoCompleteResult(String query, List<String> suggestions) {
    return new AutoCompleteResult().setQuery(query).setSuggestions(new StringArray(suggestions));
  }
}
