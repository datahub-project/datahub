package com.linkedin.metadata.search.elasticsearch.query.template;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Mustache names produced for template contract version {@link
 * SearchTemplateCapabilities#API_VERSION}. {@link #STABLE} is the public context. {@link #INTERNAL}
 * names are still filled so older bodies keep rendering, and {@code synonymPriority} is set only in
 * the write-time sample. {@link #NESTED} names are fields of a list element, not top-level keys.
 * {@link #PATHS} are dotted lookups under a stable object.
 */
public final class SearchTemplateContextVariables {
  public static final String QUERY = "query";
  public static final String RAW = "raw";
  public static final String UNQUOTED = "unquoted";
  public static final String ESCAPED = "escaped";
  public static final String NORMALIZED = "normalized";
  public static final String ALPHANUMERIC_SPLIT = "alphanumericSplit";
  public static final String FUZZY = "fuzzy";
  public static final String BROAD_QUERY = "broadQuery";
  public static final String SHORTENED = "shortened";
  public static final String LIGHT_QUERY_TEXT = "lightQueryText";
  public static final String LAST_SEGMENT = "lastSegment";
  public static final String NORMALIZED_LAST_SEGMENT = "normalizedLastSegment";
  public static final String NORMALIZED_KEYWORD = "normalizedKeyword";
  public static final String WILDCARD_TERM = "wildcardTerm";
  public static final String WHOLE_IDENTIFIER = "wholeIdentifier";
  public static final String LETTER_DIGIT_QUERY = "letterDigitQuery";
  public static final String FQN_QUERY = "fqnQuery";
  public static final String DEFAULT_QUERY_FIELDS_JSON = "defaultQueryFieldsJson";
  public static final String DEFAULT_FIELDS_WITHOUT_QUALIFIED_NAME_JSON =
      "defaultFieldsWithoutQualifiedNameJson";
  public static final String IDENTITY_QUERY_FIELDS_JSON = "identityQueryFieldsJson";
  public static final String NAME_FIELDS_JSON = "nameFieldsJson";
  public static final String QUALIFIED_NAME_FIELDS = "qualifiedNameFields";
  public static final String QUALIFIED_NAME_FIELDS_JSON = "qualifiedNameFieldsJson";
  public static final String LIGHT_FIELDS_JSON = "lightFieldsJson";
  public static final String LIGHT_OPERATOR = "lightOperator";
  public static final String ENTITY_NAME_FIELD = "entityNameField";
  public static final String NAME_KEYWORD = "nameKeyword";
  public static final String NAME_TEXT = "nameText";
  public static final String WILDCARD_FIELDS = "wildcardFields";
  public static final String PREFIX_FIELDS = "prefixFields";
  public static final String QUALIFIED_NAME_KEYWORD = "qualifiedNameKeyword";
  public static final String QUALIFIED_NAME_TEXT = "qualifiedNameText";
  public static final String QUALIFIED_NAME_IDENTITY = "qualifiedNameIdentity";
  public static final String DESCRIPTION_FIELD = "descriptionField";
  public static final String DESCRIPTION_TEXT = "descriptionText";
  public static final String PLATFORM_FIELD = "platformField";
  public static final String COLUMN_NAME_FIELD = "columnNameField";
  public static final String ID_KEYWORD = "idKeyword";
  public static final String URN_EXACT_FIELD = "urnExactField";
  public static final String URN_FOLDED_FIELD = "urnFoldedField";
  public static final String URN_PARSED_FIELD = "urnParsedField";
  public static final String URN_KEYWORD = "urnKeyword";
  public static final String PREFIX_FACTOR = "prefixFactor";
  public static final String EXACT_FACTOR = "exactFactor";
  public static final String CASE_SENSITIVITY_FACTOR = "caseSensitivityFactor";
  public static final String TWO_GRAM_FACTOR = "twoGramFactor";
  public static final String THREE_GRAM_FACTOR = "threeGramFactor";
  public static final String FOUR_GRAM_FACTOR = "fourGramFactor";
  public static final String URN_FACTOR = "urnFactor";
  public static final String PARTIAL_FACTOR = "partialFactor";
  public static final String FUZZY_MAX_EXPANSIONS = "fuzzyMaxExpansions";
  public static final String PREFIX_MAX_EXPANSIONS = "prefixMaxExpansions";
  public static final String PREFIX_FIELD = "prefixField";
  public static final String STRUCTURED_QUERY = "structuredQuery";
  public static final String FLAGS = "flags";
  public static final String SYNONYMS = "synonyms";

  /**
   * Text of one {@link #SYNONYMS} element. {@code {{.}}} is that text at the synonym section. A
   * nested section must use this name, because it replaces the current element.
   */
  public static final String SYNONYM = "synonym";

  public static final String MULTI_WORD = "multiWord";
  public static final String LONG_QUERY = "longQuery";
  public static final String DOTTED_QUERY = "dottedQuery";
  public static final String FULLY_QUALIFIED_NAME = "fullyQualifiedName";
  public static final String DEEP_PATH = "deepPath";
  public static final String LONG_EXACT_NAME = "longExactName";
  public static final String OPERATOR_ESCAPED = "operatorEscaped";
  public static final String SPACELESS_IDENTIFIER = "spacelessIdentifier";
  public static final String SPACELESS_DOTTED = "spacelessDotted";
  public static final String TERMINAL_SEGMENT = "terminalSegment";
  public static final String DOTTED_PATH = "dottedPath";
  public static final String DOTTED_PATH_PREFIX = "dottedPathPrefix";
  public static final String DOTTED_PATH_SUFFIX = "dottedPathSuffix";
  public static final String DOTTED_PATH_CONTAINS = "dottedPathContains";
  public static final String QUALIFIER_PATH = "qualifierPath";
  public static final String QUALIFIER_PATH_PREFIX = "qualifierPathPrefix";
  public static final String QUALIFIER_PATH_SUFFIX = "qualifierPathSuffix";
  public static final String QUALIFIER_PATH_CONTAINS = "qualifierPathContains";
  public static final String LETTER_DIGIT_PARTS = "letterDigitParts";
  public static final String FQN_PARTS = "fqnParts";
  public static final String ENTITY_TYPES = "entityTypes";
  public static final String VALUE_SCORE_FUNCTIONS = "valueScoreFunctions";
  public static final String FIELD_SCORE_FUNCTIONS = "fieldScoreFunctions";
  public static final String DEFAULT_QUERY_FIELDS = "defaultQueryFields";
  public static final String UNQUOTED_QUERY = "unquotedQuery";
  public static final String FUZZY_QUERY = "fuzzyQuery";
  public static final String SYNONYM_QUERIES = "synonymQueries";
  public static final String LONG_PHRASE = "longPhrase";
  public static final String DOTTED = "dotted";
  public static final String URN_FIELD = "urnField";
  public static final String SYNONYM_PRIORITY = "synonymPriority";
  public static final String FIELD = "field";
  public static final String VALUE_JSON = "valueJson";
  public static final String WEIGHT = "weight";
  public static final String FACTOR = "factor";
  public static final String MISSING = "missing";
  public static final String MODIFIER = "modifier";
  public static final String BOOST = "boost";
  public static final String EXACT_NAME_LETTER_DIGIT = "exactNameLetterDigit";
  public static final String FLAGS_ALPHANUMERIC_SPLIT = "flags.alphanumericSplit";
  public static final String FLAGS_EXACT_NAME_LETTER_DIGIT = "flags.exactNameLetterDigit";

  public static final Set<String> STABLE =
      names(
          QUERY,
          RAW,
          UNQUOTED,
          ESCAPED,
          NORMALIZED,
          ALPHANUMERIC_SPLIT,
          FUZZY,
          BROAD_QUERY,
          SHORTENED,
          LIGHT_QUERY_TEXT,
          LAST_SEGMENT,
          NORMALIZED_LAST_SEGMENT,
          NORMALIZED_KEYWORD,
          WILDCARD_TERM,
          WHOLE_IDENTIFIER,
          LETTER_DIGIT_QUERY,
          FQN_QUERY,
          DEFAULT_QUERY_FIELDS_JSON,
          DEFAULT_FIELDS_WITHOUT_QUALIFIED_NAME_JSON,
          IDENTITY_QUERY_FIELDS_JSON,
          NAME_FIELDS_JSON,
          QUALIFIED_NAME_FIELDS,
          QUALIFIED_NAME_FIELDS_JSON,
          LIGHT_FIELDS_JSON,
          LIGHT_OPERATOR,
          ENTITY_NAME_FIELD,
          NAME_KEYWORD,
          NAME_TEXT,
          WILDCARD_FIELDS,
          PREFIX_FIELDS,
          QUALIFIED_NAME_KEYWORD,
          QUALIFIED_NAME_TEXT,
          QUALIFIED_NAME_IDENTITY,
          DESCRIPTION_FIELD,
          DESCRIPTION_TEXT,
          PLATFORM_FIELD,
          COLUMN_NAME_FIELD,
          ID_KEYWORD,
          URN_EXACT_FIELD,
          URN_FOLDED_FIELD,
          URN_PARSED_FIELD,
          URN_KEYWORD,
          PREFIX_FACTOR,
          EXACT_FACTOR,
          CASE_SENSITIVITY_FACTOR,
          TWO_GRAM_FACTOR,
          THREE_GRAM_FACTOR,
          FOUR_GRAM_FACTOR,
          URN_FACTOR,
          PARTIAL_FACTOR,
          FUZZY_MAX_EXPANSIONS,
          PREFIX_MAX_EXPANSIONS,
          PREFIX_FIELD,
          STRUCTURED_QUERY,
          FLAGS,
          SYNONYMS,
          MULTI_WORD,
          LONG_QUERY,
          DOTTED_QUERY,
          FULLY_QUALIFIED_NAME,
          DEEP_PATH,
          LONG_EXACT_NAME,
          OPERATOR_ESCAPED,
          SPACELESS_IDENTIFIER,
          SPACELESS_DOTTED,
          TERMINAL_SEGMENT,
          DOTTED_PATH,
          DOTTED_PATH_PREFIX,
          DOTTED_PATH_SUFFIX,
          DOTTED_PATH_CONTAINS,
          QUALIFIER_PATH,
          QUALIFIER_PATH_PREFIX,
          QUALIFIER_PATH_SUFFIX,
          QUALIFIER_PATH_CONTAINS,
          LETTER_DIGIT_PARTS,
          FQN_PARTS,
          ENTITY_TYPES,
          VALUE_SCORE_FUNCTIONS,
          FIELD_SCORE_FUNCTIONS,
          SearchTemplateViewer.URN,
          SearchTemplateViewer.GROUPS,
          SearchTemplateViewer.DOMAINS,
          SearchTemplateViewer.DOMAIN_LIST);

  /**
   * Aliases of a stable name, plus {@code synonymPriority}, which the write-time sample sets and
   * search execution does not.
   */
  public static final Set<String> INTERNAL =
      names(
          DEFAULT_QUERY_FIELDS,
          UNQUOTED_QUERY,
          FUZZY_QUERY,
          SYNONYM_QUERIES,
          LONG_PHRASE,
          DOTTED,
          URN_FIELD,
          SYNONYM_PRIORITY);

  /** Fields of one list element, not top-level keys. */
  public static final Set<String> NESTED =
      names(FIELD, VALUE_JSON, WEIGHT, FACTOR, MISSING, MODIFIER, BOOST, SYNONYM);

  /** Dotted paths a template may branch on. The parent object is a stable top-level name. */
  public static final Set<String> PATHS =
      names(FLAGS_ALPHANUMERIC_SPLIT, FLAGS_EXACT_NAME_LETTER_DIGIT);

  private SearchTemplateContextVariables() {}

  public static boolean isContextName(@Nullable String name) {
    if (name == null || name.isEmpty() || ".".equals(name)) {
      return false;
    }
    return STABLE.contains(name)
        || INTERNAL.contains(name)
        || NESTED.contains(name)
        || PATHS.contains(name);
  }

  /**
   * Every top-level key the renderer produced is a stable name, an internal alias, or a template
   * parameter. A new key fails the render until it is classified.
   */
  public static void requireProduced(
      @Nonnull Collection<String> produced, @Nullable Collection<String> parameters) {
    Set<String> allowed = parameters == null ? Set.of() : Set.copyOf(parameters);
    for (String key : produced) {
      if (STABLE.contains(key) || INTERNAL.contains(key) || allowed.contains(key)) {
        continue;
      }
      throw new SearchTemplateException("Search template context variable is not declared: " + key);
    }
  }

  @Nonnull
  private static Set<String> names(String... values) {
    return Set.copyOf(new LinkedHashSet<>(List.of(values)));
  }
}
