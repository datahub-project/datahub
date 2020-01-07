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


public class DocumentSearchDao<CLIENT> {

  private final CLIENT _client;

  public DocumentSearchDao(@Nonnull CLIENT client) {
    this._client = client;
  }

  @Nonnull
  public JsonNode search(@Nonnull String input, @Nonnull Map<String, String> requestFilters, int start, int count)
      throws RemoteInvocationException, IOException {
    CollectionResponse<? extends RecordTemplate> resp = null;
    if (_client instanceof Datasets) {
      resp = ((Datasets) _client).search(input, requestFilters, start, count);
    } else if (_client instanceof CorpUsers) {
      resp = ((CorpUsers) _client).search(input, requestFilters, start, count);
    } else {
      throw new IllegalStateException("Unexpected client type: " + _client.getClass().getName());
    }

    return collectionResponseWithMetadataToJsonNode(resp);
  }

  @Nonnull
  public JsonNode autoComplete(@Nonnull String input, @Nonnull String field,
      @Nonnull Map<String, String> requestFilters, int limit) throws RemoteInvocationException, IOException {
    AutoCompleteResult autoCompleteResult = null;
    if (_client instanceof Datasets) {
      autoCompleteResult = ((Datasets) _client).autoComplete(input, field, requestFilters, limit);
    } else if (_client instanceof CorpUsers) {
      autoCompleteResult = ((CorpUsers) _client).autocomplete(input, field, requestFilters, limit);
    } else {
      throw new IllegalStateException("Unexpected client type: " + _client.getClass().getName());
    }

    return toJsonNode(autoCompleteResult);
  }
}
