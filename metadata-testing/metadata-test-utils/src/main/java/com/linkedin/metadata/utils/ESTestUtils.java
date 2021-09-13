package com.linkedin.metadata.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;


@Slf4j
public final class ESTestUtils {

  private static ObjectMapper _objectMapper = new ObjectMapper();

  private ESTestUtils() {
  }

  @Nonnull
  public static String getWrappedQuery(@Nonnull String query) throws IOException {
    JsonNode jsonNode = _objectMapper.readTree(query);
    WrapperQueryBuilder wp = QueryBuilders.wrapperQuery(_objectMapper
        .writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode.get("query")));
    ((ObjectNode) jsonNode).replace("query", _objectMapper.readTree(wp.toString()));
    return jsonNode.toString();
  }

  @Nonnull
  public static SearchResponse getSearchResponseFromJSON(@Nonnull String response) throws Exception {
    SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
    return getSearchResponsefromNamedContents(searchModule.getNamedXContents(), response);
  }

  @Nonnull
  private static SearchResponse getSearchResponsefromNamedContents(@Nonnull List<NamedXContentRegistry.Entry> contents,
      @Nonnull String response) throws Exception {
    NamedXContentRegistry registry = new NamedXContentRegistry(contents);
    XContentParser parser =
        JsonXContent.jsonXContent.createParser(registry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, response);
    return SearchResponse.fromXContent(parser);
  }

  @Nonnull
  private static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
    Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
    map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
    map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
    map.put(LongTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
    map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
    return map.entrySet()
        .stream()
        .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()),
            entry.getValue()))
        .collect(Collectors.toList());
  }
}
