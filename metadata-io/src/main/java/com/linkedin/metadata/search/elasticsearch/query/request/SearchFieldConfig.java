package com.linkedin.metadata.search.elasticsearch.query.request;

import lombok.Builder;
import lombok.Getter;

import javax.annotation.Nonnull;

import java.util.Set;

import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.TEXT_SEARCH_ANALYZER;

@Builder
@Getter
public class SearchFieldConfig {
    public static final Set<String> KEYWORD_FIELDS = Set.of("urn", "runId");

    @Nonnull
    private final String fieldName;
    @Builder.Default
    private final Float boost = 1.0f;
    @Builder.Default
    private final String analyzer = TEXT_SEARCH_ANALYZER;

    public static SearchFieldConfig detectSubFieldType(String fieldName, Float boost) {
        if (fieldName.endsWith(".keyword") || KEYWORD_FIELDS.contains(fieldName)) {
            return SearchFieldConfig.builder()
                    .fieldName(fieldName)
                    .boost(boost)
                    .analyzer("keyword")
                    .build();
        } else {
            return SearchFieldConfig.builder()
                    .fieldName(fieldName)
                    .boost(boost)
                    .build();
        }
    }
}
