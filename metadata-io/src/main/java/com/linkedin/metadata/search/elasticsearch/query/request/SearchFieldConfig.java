package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import lombok.Builder;
import lombok.Getter;

import javax.annotation.Nonnull;

import java.util.Set;

import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.BROWSE_PATH_HIERARCHY_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.KEYWORD_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.TEXT_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.URN_SEARCH_ANALYZER;

@Builder
@Getter
public class SearchFieldConfig {
    public static final float DEFAULT_BOOST = 1.0f;

    public static final Set<String> KEYWORD_FIELDS = Set.of("urn", "runId");

    // These should not be used directly since there is a specific
    // order in which these rules need to be evaluated for exceptions to
    // the rules.
    private static final Set<SearchableAnnotation.FieldType> TYPES_WITH_DELIMITED_SUBFIELD =
            Set.of(
                    SearchableAnnotation.FieldType.TEXT,
                    SearchableAnnotation.FieldType.TEXT_PARTIAL
                    // NOT URN_PARTIAL (urn field is special)
            );
    // NOT comprehensive
    private static final Set<SearchableAnnotation.FieldType> TYPES_WITH_KEYWORD_SUBFIELD =
            Set.of(
                    SearchableAnnotation.FieldType.URN,
                    SearchableAnnotation.FieldType.KEYWORD,
                    SearchableAnnotation.FieldType.URN_PARTIAL
            );
    private static final Set<SearchableAnnotation.FieldType> TYPES_WITH_BROWSE_PATH =
            Set.of(
                    SearchableAnnotation.FieldType.BROWSE_PATH
            );
    private static final Set<SearchableAnnotation.FieldType> TYPES_WITH_BASE_KEYWORD =
            Set.of(
                    SearchableAnnotation.FieldType.TEXT,
                    SearchableAnnotation.FieldType.TEXT_PARTIAL,
                    SearchableAnnotation.FieldType.KEYWORD,
                    // not analyzed
                    SearchableAnnotation.FieldType.BOOLEAN,
                    SearchableAnnotation.FieldType.COUNT,
                    SearchableAnnotation.FieldType.DATETIME,
                    SearchableAnnotation.FieldType.OBJECT
            );
    // NOT true for `urn`
    public static final Set<SearchableAnnotation.FieldType> TYPES_WITH_URN_TEXT =
            Set.of(
                    SearchableAnnotation.FieldType.URN,
                    SearchableAnnotation.FieldType.URN_PARTIAL
            );

    @Nonnull
    private final String fieldName;
    @Builder.Default
    private final Float boost = DEFAULT_BOOST;
    private final String analyzer;
    private boolean hasKeywordSubfield;
    private boolean hasDelimitedSubfield;

    public static SearchFieldConfig detectSubFieldType(@Nonnull SearchableFieldSpec fieldSpec) {
        final String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
        final float boost = (float) fieldSpec.getSearchableAnnotation().getBoostScore();
        final SearchableAnnotation.FieldType fieldType = fieldSpec.getSearchableAnnotation().getFieldType();
        return detectSubFieldType(fieldName, boost, fieldType);
    }

    public static SearchFieldConfig detectSubFieldType(String fieldName,
                                                       SearchableAnnotation.FieldType fieldType) {
        return detectSubFieldType(fieldName, DEFAULT_BOOST, fieldType);
    }

    public static SearchFieldConfig detectSubFieldType(String fieldName, float boost,
                                                       SearchableAnnotation.FieldType fieldType) {
        return SearchFieldConfig.builder()
                .fieldName(fieldName)
                .boost(boost)
                .analyzer(getAnalyzer(fieldName, fieldType))
                .hasKeywordSubfield(hasKeywordSubfield(fieldName, fieldType))
                .hasDelimitedSubfield(hasDelimitedSubfield(fieldName, fieldType))
                .build();
    }

    public boolean hasDelimitedSubfield() {
        return isHasDelimitedSubfield();
    }

    public boolean hasKeywordSubfield() {
        return isHasKeywordSubfield();
    }

    private static boolean hasDelimitedSubfield(String fieldName, SearchableAnnotation.FieldType fieldType) {
        return !fieldName.contains(".")
                && ("urn".equals(fieldName) || TYPES_WITH_DELIMITED_SUBFIELD.contains(fieldType));
    }
    private static boolean hasKeywordSubfield(String fieldName, SearchableAnnotation.FieldType fieldType) {
        return !"urn".equals(fieldName)
                && !fieldName.contains(".")
                && (TYPES_WITH_DELIMITED_SUBFIELD.contains(fieldType) // if delimited then also has keyword
                    || TYPES_WITH_KEYWORD_SUBFIELD.contains(fieldType));
    }
    private static boolean isKeyword(String fieldName, SearchableAnnotation.FieldType fieldType) {
        return fieldName.equals(".keyword")
                || KEYWORD_FIELDS.contains(fieldName);
    }

    private static String getAnalyzer(String fieldName, SearchableAnnotation.FieldType fieldType) {
        // order is important
        if (TYPES_WITH_BROWSE_PATH.contains(fieldType)) {
            return BROWSE_PATH_HIERARCHY_ANALYZER;
        // sub fields
        } else if (isKeyword(fieldName, fieldType)) {
            return KEYWORD_ANALYZER;
        } else if (fieldName.endsWith(".delimited")) {
            return TEXT_SEARCH_ANALYZER;
        // non-subfield cases below
        } else if (TYPES_WITH_BASE_KEYWORD.contains(fieldType)) {
            return KEYWORD_ANALYZER;
        } else if (TYPES_WITH_URN_TEXT.contains(fieldType)) {
            return URN_SEARCH_ANALYZER;
        } else {
            throw new IllegalStateException(String.format("Unknown analyzer for fieldName: %s, fieldType: %s", fieldName, fieldType));
        }
    }
}
