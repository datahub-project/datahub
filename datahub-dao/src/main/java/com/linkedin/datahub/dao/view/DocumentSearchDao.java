package com.linkedin.datahub.dao.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.identity.client.CorpUsers;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.CollectionResponse;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.util.RestliUtil.*;
import static com.linkedin.datahub.util.SearchUtil.toGraphQLAutoCompleteResponse;
import static com.linkedin.datahub.util.SearchUtil.toGraphQLSearchResponse;


public class DocumentSearchDao<CLIENT> {

  private final CLIENT _client;

  public DocumentSearchDao(@Nonnull CLIENT client) {
    this._client = client;
  }

  @Nonnull
  public JsonNode search(@Nonnull String input, @Nonnull Map<String, String> requestFilters, int start, int count)
      throws RemoteInvocationException, IOException {
    return collectionResponseWithMetadataToJsonNode(executeSearch(
            input,
            requestFilters,
            start,
            count
    ));
  }

  @Nonnull
  public Map<String, Object> graphQLSearch(@Nonnull String query,
                                           @Nonnull Map<String, String> facetFilters,
                                           int start,
                                           int count) throws RemoteInvocationException {
    return toGraphQLSearchResponse(executeSearch(
            query,
            facetFilters,
            start,
            count
    ));
  }

  @Nonnull
  public JsonNode autoComplete(@Nonnull String input, @Nonnull String field,
      @Nonnull Map<String, String> requestFilters, int limit) throws RemoteInvocationException, IOException {
    return toJsonNode(executeAutoComplete(input, field, requestFilters, limit));
  }

  @Nonnull
  public Map<String, Object> graphQLAutoComplete(@Nonnull String query,
                                                 @Nonnull String field,
                                                 @Nonnull Map<String, String> facetFilters,
                                                 int limit) throws RemoteInvocationException {
    return toGraphQLAutoCompleteResponse(executeAutoComplete(query, field, facetFilters, limit));
  }

  private CollectionResponse<? extends RecordTemplate> executeSearch(
          @Nonnull String input,
          @Nonnull Map<String, String> facetFilters,
          int start,
          int count)
          throws RemoteInvocationException {
    CollectionResponse<? extends RecordTemplate> resp;
    if (_client instanceof Datasets) {
      resp = ((Datasets) _client).search(input, facetFilters, start, count);
    } else if (_client instanceof CorpUsers) {
      resp = ((CorpUsers) _client).search(input, facetFilters, start, count);
    } else {
      throw new IllegalStateException("Unexpected client type: " + _client.getClass().getName());
    }
    return resp;
  }

  private AutoCompleteResult executeAutoComplete(
          @Nonnull String query,
          @Nonnull String field,
          @Nonnull Map<String, String> facetFilters,
          int limit)
          throws RemoteInvocationException {
    AutoCompleteResult res;
    if (_client instanceof Datasets) {
      res = ((Datasets) _client).autoComplete(query, field, facetFilters, limit);
    } else if (_client instanceof CorpUsers) {
      res = ((CorpUsers) _client).autocomplete(query, field, facetFilters, limit);
    } else {
      throw new IllegalStateException("Unexpected client type: " + _client.getClass().getName());
    }
    return res;
  }
}
