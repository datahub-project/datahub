package com.linkedin.datahub.dao.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.util.DatasetUtil;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.r2.RemoteInvocationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.util.BrowseUtil.getJsonFromBrowseResult;
import static com.linkedin.datahub.util.BrowseUtil.toGraphQLBrowseResponse;
import static com.linkedin.datahub.util.RestliUtil.*;


public class BrowseDAO<CLIENT> {
  private final CLIENT _client;

  public BrowseDAO(@Nonnull CLIENT client) {
    this._client = client;
  }

  @Nonnull
  public JsonNode browse(@Nonnull String path, @Nonnull Map<String, String> requestFilters,
      int start, int count) throws RemoteInvocationException, IOException {
    return getJsonFromBrowseResult(executeBrowse(path, requestFilters, start, count));
  }

  @Nonnull
  public Map<String, Object> graphQLBrowse(@Nonnull String path,
                                           @Nonnull Map<String, String> requestFilters,
                                           int start,
                                           int count) throws RemoteInvocationException {
    return toGraphQLBrowseResponse(executeBrowse(path, requestFilters, start, count));
  }

  @Nonnull
  public JsonNode getBrowsePaths(@Nonnull String urn) throws RemoteInvocationException, URISyntaxException {
    return stringCollectionToArrayNode(new ArrayList<>(executeBrowsePaths(urn)));
  }

  @Nonnull
  public List<String> graphQLGetBrowsePaths(@Nonnull String urn) throws RemoteInvocationException, URISyntaxException {
    return executeBrowsePaths(urn);
  }

  private BrowseResult executeBrowse(@Nonnull String path,
                                     @Nonnull Map<String, String> requestFilters,
                                     int start,
                                     int count) throws RemoteInvocationException {
    final BrowseResult res;
    if (_client instanceof Datasets) {
      res = ((Datasets) _client).browse(path, requestFilters, start, count);
    } else {
      throw new IllegalArgumentException("Unexpected client type: " + _client.getClass().getName());
    }
    return res;
  }

  private StringArray executeBrowsePaths(@Nonnull String urn) throws RemoteInvocationException, URISyntaxException {
    final StringArray res;
    if (_client instanceof Datasets) {
      res = ((Datasets) _client).getBrowsePaths(DatasetUtil.toDatasetUrn(urn));
    } else {
      throw new IllegalArgumentException("Unexpected client type: " + _client.getClass().getName());
    }
    return res;
  }
}
