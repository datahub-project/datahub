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
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.Aspect;
import com.linkedin.testing.AspectArray;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.Document;
import com.linkedin.testing.Key;
import com.linkedin.testing.Snapshot;
import com.linkedin.testing.Value;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.restli.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseSearchableEntityResourceTest extends BaseEngineTest {

  private BaseLocalDAO<Aspect, Urn> _mockLocalDAO;
  private BaseSearchDAO<Document> _mockSearchDAO;
  private TestResource _resource = new TestResource();

  class TestResource extends BaseSearchableEntityResource<Key, Value, Urn, Snapshot, Aspect, Document> {

    public TestResource() {
      super(Snapshot.class, Aspect.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<Aspect, Urn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Nonnull
    @Override
    protected BaseSearchDAO getSearchDAO() {
      return _mockSearchDAO;
    }

    @Nonnull
    @Override
    protected Urn toUrn(@Nonnull Key key) {
      return makeUrn(key.getId());
    }

    @Nonnull
    @Override
    protected Key toKey(@Nonnull Urn urn) {
      return new Key().setId(urn.getIdAsLong());
    }

    @Nonnull
    @Override
    protected Value toValue(@Nonnull Snapshot snapshot) {
      Value value = new Value();
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
    protected Snapshot toSnapshot(@Nonnull Value value, @Nonnull Urn urn) {
      Snapshot snapshot = new Snapshot().setUrn(urn);
      AspectArray aspects = new AspectArray();
      if (value.hasFoo()) {
        aspects.add(ModelUtils.newAspectUnion(Aspect.class, value.getFoo()));
      }
      if (value.hasBar()) {
        aspects.add(ModelUtils.newAspectUnion(Aspect.class, value.getBar()));
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
    Filter filter = new Filter().setCriteria(new CriterionArray());
    SearchResultMetadata searchResultMetadata = makeSearchResultMetadata(new AggregationMetadata().setName("agg")
        .setAggregations(new LongMap(ImmutableMap.of("bucket1", 1L, "bucket2", 2L))));

    when(_mockSearchDAO.search("bar", filter, 1, 2)).thenReturn(
        makeSearchResult(ImmutableList.of(makeDocument(urn1), makeDocument(urn2)), searchResultMetadata));

    String[] aspectNames = new String[]{ModelUtils.getAspectName(AspectFoo.class)};
    when(_mockLocalDAO.get(ImmutableSet.of(aspectKey1, aspectKey2))).thenReturn(
        ImmutableMap.of(aspectKey1, Optional.of(foo), aspectKey2, Optional.empty()));

    CollectionResult<Value, SearchResultMetadata> searchResult =
        runAndWait(_resource.search("bar", aspectNames, filter, new PagingContext(1, 2)));

    List<Value> values = searchResult.getElements();
    assertEquals(values.size(), 2);
    assertEquals(values.get(0).getFoo(), foo);
    assertFalse(values.get(0).hasBar());
    assertFalse(values.get(1).hasFoo());
    assertFalse(values.get(1).hasBar());

    assertEquals(searchResult.getMetadata(), searchResultMetadata);
  }

  private SearchResult<Document> makeSearchResult(List<Document> documents, SearchResultMetadata searchResultMetadata) {
    return SearchResult.<Document>builder()
        .documentList(documents)
        .searchResultMetadata(searchResultMetadata)
        .build();
  }

  private SearchResultMetadata makeSearchResultMetadata(AggregationMetadata... aggregationMetadata) {
    return new SearchResultMetadata()
        .setSearchResultMetadatas(new AggregationMetadataArray(Arrays.asList(aggregationMetadata)));
  }

  @Test
  public void testAutocomplete() {
    Filter filter = new Filter().setCriteria(new CriterionArray());

    when(_mockSearchDAO.autoComplete("foo", "name", filter, 100)).thenReturn(
        makeAutoCompleteResult("foo", ImmutableList.of("foo0", "foo1", "foo2")));

    AutoCompleteResult result = runAndWait(_resource.autocomplete("foo", "name", filter, 100));

    assertEquals(result.getQuery(), "foo");
    assertEquals(result.getSuggestions().size(), 3);
    assertEquals(result.getSuggestions().get(0), "foo0");
    assertEquals(result.getSuggestions().get(1), "foo1");
    assertEquals(result.getSuggestions().get(2), "foo2");
  }

  private AutoCompleteResult makeAutoCompleteResult(String query, List<String> suggestions) {
    return new AutoCompleteResult().setQuery(query).setSuggestions(new StringArray(suggestions));
  }
}
