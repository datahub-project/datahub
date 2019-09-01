package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for the entity rest.li resource that supports CRUD + search methods
 *
 * See http://go/gma for more details
 *
 * @param <KEY> the resource's key type
 * @param <VALUE> the resource's value type
 * @param <URN> must be a valid {@link Urn} type for the snapshot
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <ASPECT_UNION> must be a valid aspect union type supported by the snapshot
 * @param <DOCUMENT> must be a valid search document type defined in com.linkedin.metadata.search
 */
public abstract class BaseSearchableEntityResource<
    // @formatter:off
    KEY extends RecordTemplate,
    VALUE extends RecordTemplate,
    URN extends Urn,
    SNAPSHOT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate,
    DOCUMENT extends RecordTemplate>
    // @formatter:on
    extends BaseEntityResource<KEY, VALUE, URN, SNAPSHOT, ASPECT_UNION> {

  private static final Filter EMPTY_FILTER = new Filter().setCriteria(new CriterionArray());
  private static final String MATCH_ALL = "*";

  public BaseSearchableEntityResource(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseRestliAuditor auditor) {
    super(snapshotClass, aspectUnionClass, auditor);
  }

  public BaseSearchableEntityResource(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    this(snapshotClass, aspectUnionClass, new DummyRestliAuditor(Clock.systemUTC()));
  }

  /**
   * Returns a document-specific {@link BaseSearchDAO}.
   */
  @Nonnull
  protected abstract BaseSearchDAO<DOCUMENT> getSearchDAO();

  /**
   * Returns all {@link VALUE} objects
   *
   * @param aspectNames list of aspect names that need to be returned
   * @param filter {@link Filter} to filter the search results
   * @return list of all {@link VALUE} objects obtained from search results
   */
  @RestMethod.GetAll
  @Nonnull
  public Task<List<VALUE>> getAll(@QueryParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames,
                                  @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter) {
    final Filter searchFilter = filter != null ? filter : EMPTY_FILTER;
    return RestliUtils.toTask(() -> {
      /* Make first call to get the total count search hits */
      CollectionResult<VALUE, SearchResultMetadata> result =
              getSearchQueryCollectionResult(MATCH_ALL, aspectNames, searchFilter, new PagingContext(0, 0));

      /* Get all elements */
      return getSearchQueryCollectionResult(MATCH_ALL, aspectNames,
              searchFilter, new PagingContext(0, result.getTotal())).getElements();
    });
  }

  @Finder(FINDER_SEARCH)
  @Nonnull
  public Task<CollectionResult<VALUE, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @PagingContextParam @Nonnull PagingContext pagingContext) {

    final Filter searchFilter = filter != null ? filter : EMPTY_FILTER;
    return RestliUtils.toTask(
            () -> getSearchQueryCollectionResult(input, aspectNames, searchFilter, pagingContext));
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Nonnull
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Nonnull String field, @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit) {
    final Filter searchFilter = filter != null ? filter : EMPTY_FILTER;
    return RestliUtils.toTask(() -> getSearchDAO().autoComplete(query, field, searchFilter, limit));
  }

  @Nonnull
  private CollectionResult<VALUE, SearchResultMetadata> getSearchQueryCollectionResult(
          @Nonnull String input,
          @Nonnull String[] aspectNames,
          @Nullable Filter searchFilter,
          @Nonnull PagingContext pagingContext) {

    final SearchResult<DOCUMENT> searchResult =
            getSearchDAO().search(input, searchFilter, pagingContext.getStart(), pagingContext.getCount());
    final List<URN> matchedUrns = searchResult.getDocumentList()
            .stream()
            .map(d -> (URN) ModelUtils.getUrnFromDocument(d))
            .collect(Collectors.toList());
    final Map<URN, VALUE> urnValueMap = getInternal(matchedUrns, aspectClasses(aspectNames));
    return new CollectionResult<>(matchedUrns.stream().map(urn -> urnValueMap.get(urn)).collect(Collectors.toList()),
            searchResult.getTotalCount(),
            searchResult.getSearchResultMetadata());
  }
}
