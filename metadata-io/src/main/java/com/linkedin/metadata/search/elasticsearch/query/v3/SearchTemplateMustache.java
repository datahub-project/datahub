package com.linkedin.metadata.search.elasticsearch.query.v3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFields;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateHoles;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateViewer;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Renders template sections. Scalar values are JSON-string escaped. */
public final class SearchTemplateMustache {
  private static final Pattern SECTION = Pattern.compile("\\{\\{#\\s*([A-Za-z0-9_]+)\\s*}}");
  private static final ObjectMapper MAPPER = new ObjectMapper();

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
    context.put("defaultQueryFields", fields);
    context.put("defaultQueryFieldsJson", json(fields));
    context.put("identityQueryFieldsJson", json(fields));
    context.put("lightFieldsJson", json(fields));
    context.put("lightOperator", "OR");
    context.put("entityTypes", List.of("dataset", "dashboard", "chart"));
    context.put("entityNameField", SearchTemplateFields.keywordPath("name"));
    context.put("descriptionField", SearchTemplateFields.textPath("description"));
    context.put("platformField", SearchTemplateFields.PLATFORM_FIELD);
    context.put("columnNameField", SearchTemplateFields.keywordPath("columns"));
    context.put("urnField", "urn");
    context.put("query", "orders");
    context.put("raw", "orders");
    context.put("escaped", "orders");
    context.put("normalized", "orders");
    context.put("alphanumericSplit", "orders");
    context.put("unquotedQuery", "orders");
    context.put("unquoted", "orders");
    context.put("prefixFactor", 1.1d);
    context.put("exactFactor", 16.0d);
    context.put("caseSensitivityFactor", 0.0d);
    context.put("twoGramFactor", 1.2d);
    context.put("threeGramFactor", 1.5d);
    context.put("fourGramFactor", 1.8d);
    context.put("urnFactor", 0.5d);
    context.put("partialFactor", 0.4d);
    context.put("fuzzyQuery", "orders");
    context.put("fuzzy", "orders");
    context.put("lightQueryText", "orders");
    context.put("shortened", "orders");
    context.put("lastSegment", "orders");
    context.put("nameKeyword", SearchTemplateFields.keywordPath("name"));
    context.put("nameText", SearchTemplateFields.textPath("name"));
    context.put("qualifiedNameKeyword", SearchTemplateFields.keywordPath("qualifiedName"));
    context.put("qualifiedNameText", SearchTemplateFields.textPath("qualifiedName"));
    context.put("qualifiedNameIdentity", "qualifiedName");
    context.put("descriptionText", SearchTemplateFields.textPath("description"));
    context.put("idKeyword", SearchTemplateFields.keywordPath("id"));
    context.put("urnKeyword", "urn");
    context.put("letterDigitQuery", "orders 2017");
    context.put("fqnQuery", "db schema table");
    context.put("wholeIdentifier", "orders2017");
    context.put("wildcardTerm", "orders");
    context.put("synonymQueries", List.of("production"));
    context.put("synonyms", List.of("production"));
    context.put("synonymPriority", Boolean.TRUE);
    context.put("multiWord", Boolean.TRUE);
    context.put("longQuery", Boolean.TRUE);
    context.put("longPhrase", Boolean.TRUE);
    context.put("dottedQuery", Boolean.TRUE);
    context.put("dotted", Boolean.TRUE);
    context.put("fullyQualifiedName", Boolean.TRUE);
    context.put("deepPath", Boolean.TRUE);
    context.put("longExactName", Boolean.TRUE);
    context.put("spacelessIdentifier", Boolean.TRUE);
    context.put("operatorEscaped", Boolean.TRUE);
    context.put("letterDigitParts", Boolean.TRUE);
    context.put("fqnParts", Boolean.TRUE);
    Map<String, Object> flags = new LinkedHashMap<>();
    flags.put("alphanumericSplit", Boolean.TRUE);
    context.put("flags", flags);
    context.put(SearchTemplateViewer.URN, "urn:li:corpuser:unknown");
    context.put(SearchTemplateViewer.GROUPS, "[]");
    context.put(SearchTemplateViewer.DOMAINS, "[]");
    context.put(SearchTemplateViewer.DOMAIN_LIST, "[]");
    Map<String, Object> valueFunction = new LinkedHashMap<>();
    valueFunction.put("field", "name");
    valueFunction.put("valueJson", json("production"));
    valueFunction.put("weight", 1.0d);
    context.put("valueScoreFunctions", List.of(valueFunction));
    Map<String, Object> fieldFunction = new LinkedHashMap<>();
    fieldFunction.put("field", "feature");
    fieldFunction.put("factor", 1.0d);
    fieldFunction.put("missing", 0.0d);
    fieldFunction.put("modifier", "log1p");
    context.put("fieldScoreFunctions", List.of(fieldFunction));
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

  @Nonnull
  public static String json(@Nonnull Object value) {
    try {
      return MAPPER.writeValueAsString(value);
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
