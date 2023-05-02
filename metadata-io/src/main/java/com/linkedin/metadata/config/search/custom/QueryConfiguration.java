package com.linkedin.metadata.config.search.custom;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@Slf4j
@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
@JsonDeserialize(builder = QueryConfiguration.QueryConfigurationBuilder.class)
public class QueryConfiguration {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
    private static final NamedXContentRegistry X_CONTENT_REGISTRY;
    static {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        X_CONTENT_REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    private String queryRegex;
    @Builder.Default
    private boolean simpleQuery = true;
    @Builder.Default
    private boolean exactMatchQuery = true;
    @Builder.Default
    private boolean prefixMatchQuery = true;
    private BoolQueryConfiguration boolQuery;
    private Map<String, Object> functionScore;

    public FunctionScoreQueryBuilder functionScoreQueryBuilder(QueryBuilder queryBuilder) {
        return toFunctionScoreQueryBuilder(queryBuilder, functionScore);
    }

    public Optional<BoolQueryBuilder> boolQueryBuilder(String query) {
        if (boolQuery != null) {
            log.debug("Using custom query configuration queryRegex: {}", queryRegex);
        }
        return Optional.ofNullable(boolQuery).map(bq -> toBoolQueryBuilder(query, bq));
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static class QueryConfigurationBuilder {
    }

    private static BoolQueryBuilder toBoolQueryBuilder(String query, BoolQueryConfiguration boolQuery) {
        try {
            String jsonFragment = OBJECT_MAPPER.writeValueAsString(boolQuery)
                    .replace("\"{{query_string}}\"", OBJECT_MAPPER.writeValueAsString(query));
            XContentParser parser = XContentType.JSON.xContent().createParser(X_CONTENT_REGISTRY,
                    LoggingDeprecationHandler.INSTANCE, jsonFragment);
            return BoolQueryBuilder.fromXContent(parser);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static FunctionScoreQueryBuilder toFunctionScoreQueryBuilder(QueryBuilder queryBuilder,
                                                                         Map<String, Object> params) {
        try {
            HashMap<String, Object> body = new HashMap<>(params);
            if (!body.isEmpty()) {
                log.debug("Using custom scoring functions: {}", body);
            }

            body.put("query", OBJECT_MAPPER.readValue(queryBuilder.toString(), Map.class));

            String jsonFragment = OBJECT_MAPPER.writeValueAsString(Map.of(
                    "function_score", body
            ));
            XContentParser parser = XContentType.JSON.xContent().createParser(X_CONTENT_REGISTRY,
                    LoggingDeprecationHandler.INSTANCE, jsonFragment);
            return (FunctionScoreQueryBuilder) FunctionScoreQueryBuilder.parseInnerQueryBuilder(parser);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
