package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.linkedin.metadata.models.EntitySpec;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;


@Slf4j
public class EntityIndexBuilder extends IndexBuilder {
  private final EntitySpec entitySpec;

  public EntityIndexBuilder(RestHighLevelClient searchClient, EntitySpec entitySpec, String indexName) {
    super(searchClient, indexName, MappingsBuilder.getMappings(entitySpec), SettingsBuilder.getSettings());
    this.entitySpec = entitySpec;
  }
}
