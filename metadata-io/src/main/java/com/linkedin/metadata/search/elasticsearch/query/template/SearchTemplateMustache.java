package com.linkedin.metadata.search.elasticsearch.query.template;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Renders template sections. Scalar values are JSON-string escaped.
 *
 * <p>{@code validationContext} is a write-time fixture. It fills every context key with one sample
 * so a proposed template can be rendered and rejected when a section is unknown or the result is
 * not JSON. A search does not use these samples. It builds a separate context from the request.
 */
public final class SearchTemplateMustache {
  private static final Pattern SECTION = Pattern.compile("\\{\\{#\\s*([A-Za-z0-9_]+)\\s*}}");

  private static final MustacheFactory FACTORY =
      new DefaultMustacheFactory(
          name -> {
            throw new SearchTemplateException("Search template partials are not allowed");
          }) {
        @Override
        public void encode(String value, Writer writer) {
          try {
            writer.write(jsonEscape(value));
          } catch (IOException e) {
            throw new SearchTemplateException("Failed to encode a template value", e);
          }
        }
      };

  private static final Cache<String, Mustache> COMPILED =
      Caffeine.newBuilder().maximumSize(256).build();

  private SearchTemplateMustache() {}

  @Nonnull
  public static String render(@Nullable String template, @Nonnull Map<String, Object> context) {
    if (template == null) {
      return "";
    }
    try {
      Mustache mustache =
          COMPILED.get(template, body -> FACTORY.compile(new StringReader(body), "searchTemplate"));
      StringWriter writer = new StringWriter();
      mustache.execute(writer, context).flush();
      return writer.toString();
    } catch (IOException e) {
      throw new SearchTemplateException("Failed to render search template", e);
    } catch (RuntimeException e) {
      throw new SearchTemplateException("Failed to render search template", e);
    }
  }

  /**
   * Context used at write time. Schema lists and score functions are one realistic sample. A {@code
   * {{#section}}} that is not in this context and is not a splice fails before render.
   */
  @Nonnull
  public static Map<String, Object> validationContext(@Nonnull String source) {
    Map<String, Object> context = new LinkedHashMap<>();
    List<String> fields = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      fields.add("field" + i + "^1.0");
    }
    context.put(SearchTemplateContextVariables.DEFAULT_QUERY_FIELDS, fields);
    context.put(SearchTemplateContextVariables.DEFAULT_QUERY_FIELDS_JSON, json(fields));
    context.put(
        SearchTemplateContextVariables.DEFAULT_FIELDS_WITHOUT_QUALIFIED_NAME_JSON, json(fields));
    context.put(SearchTemplateContextVariables.IDENTITY_QUERY_FIELDS_JSON, json(fields));
    context.put(SearchTemplateContextVariables.NAME_FIELDS_JSON, json(List.of("name.text^10.0")));
    context.put(SearchTemplateContextVariables.QUALIFIED_NAME_FIELDS, Boolean.TRUE);
    context.put(
        SearchTemplateContextVariables.QUALIFIED_NAME_FIELDS_JSON,
        json(List.of("qualifiedName.text^10.0")));
    context.put(SearchTemplateContextVariables.LIGHT_FIELDS_JSON, json(fields));
    context.put(SearchTemplateContextVariables.LIGHT_OPERATOR, "OR");
    context.put(
        SearchTemplateContextVariables.ENTITY_TYPES, List.of("dataset", "dashboard", "chart"));
    context.put(
        SearchTemplateContextVariables.ENTITY_NAME_FIELD, SearchTemplateFields.keywordPath("name"));
    context.put(
        SearchTemplateContextVariables.DESCRIPTION_FIELD,
        SearchTemplateFields.textPath("description"));
    context.put(SearchTemplateContextVariables.PLATFORM_FIELD, SearchTemplateFields.PLATFORM_FIELD);
    context.put(
        SearchTemplateContextVariables.COLUMN_NAME_FIELD,
        SearchTemplateFields.keywordPath("name_1"));
    context.put(SearchTemplateContextVariables.URN_FIELD, "urn");
    context.put(SearchTemplateContextVariables.URN_EXACT_FIELD, "urn");
    context.put(SearchTemplateContextVariables.URN_FOLDED_FIELD, "urn.lowercase");
    context.put(
        SearchTemplateContextVariables.URN_PARSED_FIELD, "urn_name." + V3SearchFields.LOWERCASE);
    context.put(SearchTemplateContextVariables.QUERY, "orders");
    context.put(SearchTemplateContextVariables.RAW, "orders");
    context.put(SearchTemplateContextVariables.ESCAPED, "orders");
    context.put(SearchTemplateContextVariables.NORMALIZED, "orders");
    context.put(SearchTemplateContextVariables.ALPHANUMERIC_SPLIT, "orders");
    context.put(SearchTemplateContextVariables.UNQUOTED_QUERY, "orders");
    context.put(SearchTemplateContextVariables.UNQUOTED, "orders");
    context.put(SearchTemplateContextVariables.PREFIX_FACTOR, 1.1d);
    context.put(SearchTemplateContextVariables.EXACT_FACTOR, 16.0d);
    context.put(SearchTemplateContextVariables.CASE_SENSITIVITY_FACTOR, 0.0d);
    context.put(SearchTemplateContextVariables.TWO_GRAM_FACTOR, 1.2d);
    context.put(SearchTemplateContextVariables.THREE_GRAM_FACTOR, 1.5d);
    context.put(SearchTemplateContextVariables.FOUR_GRAM_FACTOR, 1.8d);
    context.put(SearchTemplateContextVariables.URN_FACTOR, 0.5d);
    context.put(SearchTemplateContextVariables.PARTIAL_FACTOR, 0.4d);
    context.put(SearchTemplateContextVariables.FUZZY_QUERY, "orders");
    context.put(SearchTemplateContextVariables.FUZZY, "orders");
    context.put(SearchTemplateContextVariables.FUZZY_MAX_EXPANSIONS, 10);
    context.put(SearchTemplateContextVariables.PREFIX_MAX_EXPANSIONS, 50);
    context.put(SearchTemplateContextVariables.PREFIX_FIELD, "name.text");
    context.put(SearchTemplateContextVariables.STRUCTURED_QUERY, "{\"match_all\":{}}");
    context.put(SearchTemplateContextVariables.BROAD_QUERY, "orders");
    context.put(SearchTemplateContextVariables.LIGHT_QUERY_TEXT, "orders");
    context.put(SearchTemplateContextVariables.SHORTENED, "orders");
    context.put(SearchTemplateContextVariables.LAST_SEGMENT, "orders");
    context.put(SearchTemplateContextVariables.NORMALIZED_LAST_SEGMENT, "orders");
    context.put(
        SearchTemplateContextVariables.NAME_KEYWORD, SearchTemplateFields.keywordPath("name"));
    context.put(SearchTemplateContextVariables.NAME_TEXT, SearchTemplateFields.textPath("name"));
    context.put(
        SearchTemplateContextVariables.QUALIFIED_NAME_KEYWORD,
        SearchTemplateFields.keywordPath("qualifiedName"));
    context.put(
        SearchTemplateContextVariables.QUALIFIED_NAME_TEXT,
        SearchTemplateFields.textPath("qualifiedName"));
    context.put(
        SearchTemplateContextVariables.QUALIFIED_NAME_IDENTITY,
        SearchTemplateFields.textPath("qualifiedName"));
    context.put(
        SearchTemplateContextVariables.DESCRIPTION_TEXT,
        SearchTemplateFields.textPath("description"));
    context.put(SearchTemplateContextVariables.ID_KEYWORD, SearchTemplateFields.keywordPath("id"));
    context.put(SearchTemplateContextVariables.URN_KEYWORD, "urn");
    context.put(SearchTemplateContextVariables.NORMALIZED_KEYWORD, "orders");
    context.put(SearchTemplateContextVariables.LETTER_DIGIT_QUERY, "orders 2017");
    context.put(SearchTemplateContextVariables.FQN_QUERY, "db schema table");
    context.put(SearchTemplateContextVariables.WHOLE_IDENTIFIER, "orders2017");
    context.put(SearchTemplateContextVariables.WILDCARD_TERM, "orders");
    Map<String, Object> nameWildcard = new LinkedHashMap<>();
    nameWildcard.put(SearchTemplateContextVariables.FIELD, "name.text");
    nameWildcard.put(SearchTemplateContextVariables.BOOST, 0.3d);
    Map<String, Object> titleWildcard = new LinkedHashMap<>();
    titleWildcard.put(SearchTemplateContextVariables.FIELD, "title.text");
    titleWildcard.put(SearchTemplateContextVariables.BOOST, 0.3d);
    context.put(
        SearchTemplateContextVariables.WILDCARD_FIELDS, List.of(nameWildcard, titleWildcard));
    Map<String, Object> namePrefix = new LinkedHashMap<>();
    namePrefix.put(SearchTemplateContextVariables.FIELD, "name.text");
    namePrefix.put(SearchTemplateContextVariables.BOOST, 26.4d);
    context.put(SearchTemplateContextVariables.PREFIX_FIELDS, List.of(namePrefix));
    List<String> synonyms =
        Collections.nCopies(SearchTemplateNormalizationPolicy.MAX_SYNONYM_CAP, "production");
    context.put(SearchTemplateContextVariables.SYNONYM_QUERIES, synonyms);
    context.put(SearchTemplateContextVariables.SYNONYMS, synonymElements(synonyms));
    context.put(SearchTemplateContextVariables.SYNONYM_PRIORITY, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.MULTI_WORD, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.LONG_QUERY, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.LONG_PHRASE, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.DOTTED_QUERY, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.DOTTED, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.FULLY_QUALIFIED_NAME, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.DEEP_PATH, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.LONG_EXACT_NAME, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.SPACELESS_IDENTIFIER, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.SPACELESS_DOTTED, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.TERMINAL_SEGMENT, "orders");
    context.put(SearchTemplateContextVariables.DOTTED_PATH, "db.orders");
    context.put(SearchTemplateContextVariables.DOTTED_PATH_PREFIX, "db.orders.*");
    context.put(SearchTemplateContextVariables.DOTTED_PATH_SUFFIX, "*.db.orders");
    context.put(SearchTemplateContextVariables.DOTTED_PATH_CONTAINS, "*.db.orders.*");
    context.put(SearchTemplateContextVariables.QUALIFIER_PATH, "db");
    context.put(SearchTemplateContextVariables.QUALIFIER_PATH_PREFIX, "db.*");
    context.put(SearchTemplateContextVariables.QUALIFIER_PATH_SUFFIX, "*.db");
    context.put(SearchTemplateContextVariables.QUALIFIER_PATH_CONTAINS, "*.db.*");
    context.put(SearchTemplateContextVariables.OPERATOR_ESCAPED, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.LETTER_DIGIT_PARTS, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.FQN_PARTS, Boolean.TRUE);
    Map<String, Object> flags = new LinkedHashMap<>();
    flags.put(SearchTemplateContextVariables.ALPHANUMERIC_SPLIT, Boolean.TRUE);
    flags.put(SearchTemplateContextVariables.EXACT_NAME_LETTER_DIGIT, Boolean.FALSE);
    context.put(SearchTemplateContextVariables.FLAGS, flags);
    context.put(SearchTemplateViewer.URN, "urn:li:corpuser:unknown");
    context.put(SearchTemplateViewer.GROUPS, "[]");
    context.put(SearchTemplateViewer.DOMAINS, "[]");
    context.put(SearchTemplateViewer.DOMAIN_LIST, "[]");
    Map<String, Object> valueFunction = new LinkedHashMap<>();
    valueFunction.put(SearchTemplateContextVariables.FIELD, "name");
    valueFunction.put(SearchTemplateContextVariables.VALUE_JSON, json("production"));
    valueFunction.put(SearchTemplateContextVariables.WEIGHT, 1.0d);
    context.put(SearchTemplateContextVariables.VALUE_SCORE_FUNCTIONS, List.of(valueFunction));
    Map<String, Object> fieldFunction = new LinkedHashMap<>();
    fieldFunction.put(SearchTemplateContextVariables.FIELD, "feature");
    fieldFunction.put(SearchTemplateContextVariables.FACTOR, 1.0d);
    fieldFunction.put(SearchTemplateContextVariables.MISSING, 0.0d);
    fieldFunction.put(SearchTemplateContextVariables.MODIFIER, "log1p");
    context.put(SearchTemplateContextVariables.FIELD_SCORE_FUNCTIONS, List.of(fieldFunction));
    SearchTemplateContextVariables.requireProduced(context.keySet(), Set.of());
    Set<String> splices = SearchTemplateHoles.splices(source);
    Set<String> allowed = recognized(context);
    Matcher matcher = SECTION.matcher(source);
    while (matcher.find()) {
      String name = matcher.group(1);
      if (splices.contains(name) || allowed.contains(name)) {
        continue;
      }
      throw new SearchTemplateException("Search template section is not recognized: " + name);
    }
    return context;
  }

  /**
   * Top-level keys, plus keys of a sample element. {@code modifier} is a field of one score
   * function, not its own list.
   */
  @Nonnull
  private static Set<String> recognized(@Nonnull Map<String, Object> context) {
    Set<String> names = new LinkedHashSet<>(context.keySet());
    for (Object value : context.values()) {
      if (!(value instanceof List<?> list)) {
        continue;
      }
      for (Object element : list) {
        if (element instanceof Map<?, ?> map) {
          for (Object key : map.keySet()) {
            names.add(String.valueOf(key));
          }
        }
      }
    }
    return names;
  }

  /**
   * One map per synonym. {@code {{synonym}}} still resolves inside a nested section such as {@code
   * {{#qualifiedNameFields}}}. {@code {{.}}} at the synonym section is the synonym text, so a
   * stored template written that way keeps searching the synonym.
   */
  @Nonnull
  public static List<Map<String, String>> synonymElements(@Nonnull List<String> synonyms) {
    List<Map<String, String>> elements = new ArrayList<>(synonyms.size());
    for (String synonym : synonyms) {
      elements.add(new SynonymElement(synonym));
    }
    return elements;
  }

  /** {@code toString} is the synonym so Mustache {@code {{.}}} writes the text, not the map. */
  private static final class SynonymElement extends LinkedHashMap<String, String> {
    private final String text;

    private SynonymElement(@Nonnull String text) {
      super(1);
      this.text = text;
      put(SearchTemplateContextVariables.SYNONYM, text);
    }

    @Override
    public String toString() {
      return text;
    }
  }

  @Nonnull
  public static String json(@Nonnull Object value) {
    try {
      return SearchTemplateMappers.JSON.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new SearchTemplateException("Failed to encode a template list", e);
    }
  }

  @Nonnull
  public static String jsonEscape(@Nullable String value) {
    if (value == null) {
      return "";
    }
    StringBuilder out = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
    return out.toString();
  }
}
