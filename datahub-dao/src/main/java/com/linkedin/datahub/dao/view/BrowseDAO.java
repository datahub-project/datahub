package com.linkedin.datahub.dao.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.util.DatasetUtil;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.BrowseResultEntityArray;
import com.linkedin.r2.RemoteInvocationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.util.RestliUtil.*;
import static java.util.stream.Collectors.toList;


public class BrowseDAO<CLIENT> {
  private final CLIENT _client;

  public BrowseDAO(@Nonnull CLIENT client) {
    this._client = client;
  }

  @Nonnull
  public JsonNode browse(@Nonnull String path, @Nonnull Map<String, String> requestFilters,
      int start, int count) throws RemoteInvocationException, IOException {
    final BrowseResult resp;
    if (_client instanceof Datasets) {
      resp = ((Datasets) _client).browse(path, requestFilters, start, count);
    } else {
      throw new IllegalArgumentException("Unexpected client type: " + _client.getClass().getName());
    }
    return getJsonFromBrowseResult(resp);
  }

  @Nonnull
  public JsonNode getBrowsePaths(@Nonnull String urn) throws RemoteInvocationException, URISyntaxException {
    final StringArray response;
    if (_client instanceof Datasets) {
      response = ((Datasets) _client).getBrowsePaths(DatasetUtil.toDatasetUrn(urn));
    } else {
      throw new IllegalArgumentException("Unexpected client type: " + _client.getClass().getName());
    }
    return stringCollectionToArrayNode(response.stream().collect(toList()));
  }

  @Nonnull
  public JsonNode getJsonFromBrowseResult(@Nonnull BrowseResult browseResult) throws IOException {
    final ObjectNode node = OM.createObjectNode();
    final BrowseResultEntityArray browseResultEntityArray = browseResult.getEntities();
    node.set("elements", collectionToArrayNode(browseResultEntityArray.subList(0, browseResultEntityArray.size())));
    node.put("start", browseResult.getFrom());
    node.put("count", browseResult.getPageSize());
    node.put("total", browseResult.getNumEntities());
    node.set("metadata", toJsonNode(browseResult.getMetadata()));
    return node;
  }
}
