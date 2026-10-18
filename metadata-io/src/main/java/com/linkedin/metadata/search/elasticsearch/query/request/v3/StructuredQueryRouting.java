package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX;

import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;

/**
 * Builds the structured {@code /q} query. An unqualified query is the shared fields, strict, OR the
 * opted-in property fields, lenient: {@code query_string} applies {@code fields()} only when the
 * text does not name a field. A fielded query ({@code name:orders}, {@code
 * structuredProperties.foo:bar}) ignores that list, so copying the text onto both clauses would
 * score the named field twice and would run a property field on the strict clause. Each named field
 * therefore runs once. A property field is lenient, so a property mapped on only some indices of a
 * multi-index search does not fail the request. Every other named field stays strict.
 */
final class StructuredQueryRouting {

  private static final int MAX_DEPTH = 32;

  private StructuredQueryRouting() {}

  static Optional<QueryBuilder> build(
      @Nonnull String query,
      @Nonnull List<SearchFieldConfig> shared,
      @Nonnull List<SearchFieldConfig> properties,
      @Nullable SearchFieldResolver fieldResolver) {
    if (query.isBlank() || !mightHaveExplicitField(query)) {
      return unqualified(query, shared, properties);
    }
    try {
      Node node = new Parser(query).parse();
      if (!node.hasExplicitField()) {
        return unqualified(query, shared, properties);
      }
      return Optional.ofNullable(node.toQuery(shared, properties, fieldResolver));
    } catch (IllegalArgumentException e) {
      // One clause still names each field once. Lenient, so a property field we could not isolate
      // does not fail a multi-index search.
      List<SearchFieldConfig> all = new ArrayList<>(shared.size() + properties.size());
      all.addAll(shared);
      all.addAll(properties);
      return Optional.ofNullable(clause(query, all, true, true));
    }
  }

  /**
   * Unqualified text keeps the historical split: every term must match within the shared fields or
   * every term must match within the property fields.
   */
  private static Optional<QueryBuilder> unqualified(
      @Nonnull String query,
      @Nonnull List<SearchFieldConfig> shared,
      @Nonnull List<SearchFieldConfig> properties) {
    QueryStringQueryBuilder sharedQuery = clause(query, shared, false, false);
    QueryStringQueryBuilder propertyQuery = clause(query, properties, true, false);
    if (sharedQuery != null && propertyQuery != null) {
      return Optional.of(
          QueryBuilders.boolQuery()
              .should(sharedQuery)
              .should(propertyQuery)
              .minimumShouldMatch(1));
    } else if (sharedQuery != null) {
      return Optional.of(sharedQuery);
    } else if (propertyQuery != null) {
      return Optional.of(propertyQuery);
    }
    return Optional.empty();
  }

  @Nullable
  private static QueryStringQueryBuilder clause(
      @Nonnull String query,
      @Nonnull List<SearchFieldConfig> fields,
      boolean lenient,
      boolean explicitField) {
    // A fielded clause names its field in the text. fields() does not choose it, and may be empty
    // when the property is not queried by default.
    if (fields.isEmpty() && !explicitField) {
      return null;
    }
    QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(query);
    queryBuilder.defaultOperator(Operator.AND);
    if (lenient) {
      queryBuilder.lenient(true);
    }
    fields.forEach(field -> queryBuilder.field(field.fieldName(), field.boost()));
    return queryBuilder;
  }

  /** A colon outside quotes. Quoted colons and escaped colons are not field syntax. */
  private static boolean mightHaveExplicitField(@Nonnull String query) {
    boolean quoted = false;
    for (int i = 0; i < query.length(); i++) {
      char c = query.charAt(i);
      if (c == '\\') {
        i++;
        continue;
      }
      if (c == '"') {
        quoted = !quoted;
        continue;
      }
      if (!quoted && c == ':') {
        return true;
      }
    }
    return false;
  }

  private static boolean isPropertyField(@Nonnull String field) {
    return field.equals(STRUCTURED_PROPERTY_MAPPING_FIELD)
        || field.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)
        || field.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD + "*");
  }

  /** Any explicit property field in {@code text}, including one nested in a field group. */
  private static boolean mentionsPropertyField(@Nonnull String text) {
    boolean quoted = false;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '\\') {
        i++;
        continue;
      }
      if (c == '"') {
        quoted = !quoted;
        continue;
      }
      if (quoted || c != ':') {
        continue;
      }
      int start = i;
      while (start > 0 && isFieldChar(text.charAt(start - 1))) {
        start--;
      }
      if (start < i && isPropertyField(unescape(text.substring(start, i)))) {
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private static String unescape(@Nonnull String field) {
    if (field.indexOf('\\') < 0) {
      return field;
    }
    StringBuilder out = new StringBuilder(field.length());
    for (int i = 0; i < field.length(); i++) {
      char c = field.charAt(i);
      if (c == '\\' && i + 1 < field.length()) {
        out.append(field.charAt(++i));
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }

  @Nullable
  private static QueryBuilder withBoost(@Nullable QueryBuilder query, float boost) {
    if (query == null || boost == 1f) {
      return query;
    }
    if (query instanceof AbstractQueryBuilder<?> builder) {
      builder.boost(builder.boost() * boost);
    }
    return query;
  }

  private abstract static class Node {
    float weight = 1f;

    abstract boolean hasExplicitField();

    @Nullable
    abstract QueryBuilder toQuery(
        List<SearchFieldConfig> shared,
        List<SearchFieldConfig> properties,
        @Nullable SearchFieldResolver fieldResolver);

    @Nullable
    final QueryBuilder finish(@Nonnull List<QueryBuilder> children, boolean or) {
      if (children.isEmpty()) {
        return null;
      }
      if (children.size() == 1) {
        return withBoost(children.get(0), weight);
      }
      BoolQueryBuilder bool = QueryBuilders.boolQuery();
      for (QueryBuilder child : children) {
        if (or) {
          bool.should(child);
        } else {
          bool.must(child);
        }
      }
      if (or) {
        bool.minimumShouldMatch(1);
      }
      return withBoost(bool, weight);
    }
  }

  private static final class And extends Node {
    private final List<Node> children;

    private And(List<Node> children) {
      this.children = children;
    }

    @Override
    boolean hasExplicitField() {
      return children.stream().anyMatch(Node::hasExplicitField);
    }

    @Override
    QueryBuilder toQuery(
        List<SearchFieldConfig> shared,
        List<SearchFieldConfig> properties,
        @Nullable SearchFieldResolver fieldResolver) {
      List<QueryBuilder> built = new ArrayList<>(children.size());
      for (Node child : children) {
        QueryBuilder query = child.toQuery(shared, properties, fieldResolver);
        if (query != null) {
          built.add(query);
        }
      }
      return finish(built, false);
    }
  }

  private static final class Or extends Node {
    private final List<Node> children;

    private Or(List<Node> children) {
      this.children = children;
    }

    @Override
    boolean hasExplicitField() {
      return children.stream().anyMatch(Node::hasExplicitField);
    }

    @Override
    QueryBuilder toQuery(
        List<SearchFieldConfig> shared,
        List<SearchFieldConfig> properties,
        @Nullable SearchFieldResolver fieldResolver) {
      List<QueryBuilder> built = new ArrayList<>(children.size());
      for (Node child : children) {
        QueryBuilder query = child.toQuery(shared, properties, fieldResolver);
        if (query != null) {
          built.add(query);
        }
      }
      return finish(built, true);
    }
  }

  private static final class Not extends Node {
    private final Node child;

    private Not(Node child) {
      this.child = child;
    }

    @Override
    boolean hasExplicitField() {
      return child.hasExplicitField();
    }

    @Override
    QueryBuilder toQuery(
        List<SearchFieldConfig> shared,
        List<SearchFieldConfig> properties,
        @Nullable SearchFieldResolver fieldResolver) {
      QueryBuilder inner = child.toQuery(shared, properties, fieldResolver);
      if (inner == null) {
        return null;
      }
      return withBoost(QueryBuilders.boolQuery().mustNot(inner), weight);
    }
  }

  private static final class Leaf extends Node {
    private final String text;
    @Nullable private final String field;

    private Leaf(String text, @Nullable String field) {
      this.text = text;
      this.field = field;
    }

    @Override
    boolean hasExplicitField() {
      return field != null;
    }

    @Override
    QueryBuilder toQuery(
        List<SearchFieldConfig> shared,
        List<SearchFieldConfig> properties,
        @Nullable SearchFieldResolver fieldResolver) {
      if (field == null) {
        return withBoost(unqualified(text, shared, properties).orElse(null), weight);
      }
      // A group such as name:(foo OR structuredProperties.bar:baz) is one query_string. Lenient
      // when it names a property, so that field is not evaluated strictly.
      boolean lenient = isPropertyField(field) || mentionsPropertyField(text);
      if (!lenient && !isQuotedFieldValue(text)) {
        QueryBuilder contained = containQuery(text, field, shared, fieldResolver);
        if (contained != null) {
          return withBoost(contained, weight);
        }
      }
      return withBoost(clause(text, lenient ? properties : shared, lenient, true), weight);
    }
  }

  /**
   * An unquoted {@code /q} value uses the same {@code CONTAIN} resolution as a contains filter. A
   * text-only label searches its analyzed subfields. A source name whose label differs, such as
   * {@code fieldPaths}, searches the keyword path. A label written as itself, such as {@code name}
   * or {@code tags}, stays as written. A quoted value is exact and is not rewritten.
   */
  @Nullable
  private static QueryBuilder containQuery(
      @Nonnull String text,
      @Nonnull String field,
      @Nonnull List<SearchFieldConfig> shared,
      @Nullable SearchFieldResolver fieldResolver) {
    if (fieldResolver == null
        || !fieldResolver.contains(field)
        || !fieldResolver.supports(field, Intent.CONTAIN)) {
      return null;
    }
    SearchFieldResolver.Field resolved = fieldResolver.resolve(field, Intent.CONTAIN);
    if (!resolved.isIndexed()) {
      List<SearchFieldConfig> analyzed = new ArrayList<>();
      for (String path : fieldResolver.analyzedPaths(field)) {
        analyzed.add(configFor(shared, path));
      }
      return analyzed.isEmpty() ? null : textOnlyQuery(text, analyzed);
    }
    if (field.equals(resolved.getPath())) {
      return null;
    }
    int colon = text.indexOf(':');
    String keywordPath = fieldResolver.exactPath(field, Intent.CONTAIN);
    return clause(keywordPath + text.substring(colon), shared, false, true);
  }

  @Nonnull
  private static SearchFieldConfig configFor(
      @Nonnull List<SearchFieldConfig> shared, @Nonnull String path) {
    for (SearchFieldConfig config : shared) {
      if (config.fieldName().equals(path)) {
        return config;
      }
    }
    return SearchFieldConfig.builder()
        .fieldName(path)
        .boost(SearchFieldConfig.DEFAULT_BOOST)
        .build();
  }

  /** A quoted value ({@code description:"data warehouse"}) stays on exact intent. */
  private static boolean isQuotedFieldValue(@Nonnull String text) {
    int colon = text.indexOf(':');
    if (colon < 0) {
      return false;
    }
    int index = colon + 1;
    while (index < text.length() && isWhitespace(text.charAt(index))) {
      index++;
    }
    return index < text.length() && text.charAt(index) == '"';
  }

  @Nonnull
  private static QueryBuilder textOnlyQuery(
      @Nonnull String text, @Nonnull List<SearchFieldConfig> analyzed) {
    int colon = text.indexOf(':');
    String value = text.substring(colon);
    List<QueryBuilder> clauses = new ArrayList<>();
    for (SearchFieldConfig subfield : analyzed) {
      QueryStringQueryBuilder query = QueryBuilders.queryStringQuery(subfield.fieldName() + value);
      query.defaultOperator(Operator.AND);
      query.boost(subfield.boost());
      clauses.add(query);
    }
    if (clauses.size() == 1) {
      return clauses.get(0);
    }
    BoolQueryBuilder bool = QueryBuilders.boolQuery().minimumShouldMatch(1);
    clauses.forEach(bool::should);
    return bool;
  }

  /**
   * Query-string syntax with the default AND operator. Enough to keep each explicit field on one
   * clause. A query this cannot parse falls back to a single lenient clause.
   */
  private static final class Parser {
    private final String query;
    private int i;
    private int depth;

    private Parser(String query) {
      this.query = query;
    }

    private Node parse() {
      Node node = parseOr();
      skipWs();
      if (i < query.length()) {
        throw new IllegalArgumentException("Unparsed structured query");
      }
      return node;
    }

    private Node parseOr() {
      if (++depth > MAX_DEPTH) {
        throw new IllegalArgumentException("Structured query is too nested");
      }
      try {
        Node first = parseAnd();
        List<Node> parts = null;
        while (true) {
          int mark = i;
          skipWs();
          if (!consumeWord("OR") && !consume("||")) {
            i = mark;
            break;
          }
          if (parts == null) {
            parts = new ArrayList<>();
            parts.add(first);
          }
          parts.add(parseAnd());
        }
        return parts == null ? first : new Or(parts);
      } finally {
        depth--;
      }
    }

    private Node parseAnd() {
      Node first = parseModifier();
      List<Node> parts = null;
      while (true) {
        int mark = i;
        skipWs();
        if (i >= query.length() || query.charAt(i) == ')') {
          i = mark;
          break;
        }
        if (lookingAt("||") || lookingAtWord("OR")) {
          i = mark;
          break;
        }
        boolean explicit = consumeWord("AND") || consume("&&");
        if (!explicit && !startsClause()) {
          i = mark;
          break;
        }
        int before = i;
        Node next = parseModifier();
        if (i == before) {
          throw new IllegalArgumentException("Structured query did not advance");
        }
        if (parts == null) {
          parts = new ArrayList<>();
          parts.add(first);
        }
        parts.add(next);
      }
      return parts == null ? first : new And(parts);
    }

    private Node parseModifier() {
      skipWs();
      if (consumeWord("NOT") || consume("!") || consume("-")) {
        return new Not(parseClause());
      }
      consume("+");
      return parseClause();
    }

    private Node parseClause() {
      skipWs();
      int start = i;
      String field = tryReadField();
      if (field != null) {
        // Query-string syntax allows whitespace between the colon and the value.
        skipWs();
        readValue();
        if (i == start) {
          throw new IllegalArgumentException("Empty fielded clause");
        }
        return new Leaf(query.substring(start, i), field);
      }
      if (lookingAt("(")) {
        i++;
        Node inner = parseOr();
        skipWs();
        if (i >= query.length() || query.charAt(i) != ')') {
          throw new IllegalArgumentException("Unclosed group");
        }
        i++;
        inner.weight *= readBoost();
        return inner;
      }
      readValue();
      if (i == start) {
        throw new IllegalArgumentException("Empty clause");
      }
      return new Leaf(query.substring(start, i), null);
    }

    /** Field name and the colon, or null when the upcoming text is not {@code field:value}. */
    @Nullable
    private String tryReadField() {
      int j = i;
      int fieldStart = j;
      boolean any = false;
      while (j < query.length()) {
        char c = query.charAt(j);
        if (c == '\\') {
          if (j + 1 >= query.length()) {
            return null;
          }
          j += 2;
          any = true;
          continue;
        }
        if (c == ':') {
          if (!any) {
            return null;
          }
          String field = unescape(query.substring(fieldStart, j));
          i = j + 1;
          return field;
        }
        if (!isFieldChar(c)) {
          return null;
        }
        any = true;
        j++;
      }
      return null;
    }

    private void readValue() {
      if (i >= query.length()) {
        throw new IllegalArgumentException("Missing field value");
      }
      char c = query.charAt(i);
      if (c == '"') {
        readQuoted();
      } else if (c == '(') {
        readBalanced('(', ')');
      } else if (c == '[') {
        readBalanced('[', ']');
      } else if (c == '{') {
        readBalanced('{', '}');
      } else if (c == '/') {
        readRegex();
      } else {
        readTerm();
      }
      readFuzzy();
      readBoost();
    }

    private void readTerm() {
      int start = i;
      while (i < query.length()) {
        char c = query.charAt(i);
        if (c == '\\') {
          i += i + 1 < query.length() ? 2 : 1;
          continue;
        }
        if (isWhitespace(c) || c == ')' || c == '^' || c == '~') {
          break;
        }
        if (c == '&' && i + 1 < query.length() && query.charAt(i + 1) == '&') {
          break;
        }
        if (c == '|' && i + 1 < query.length() && query.charAt(i + 1) == '|') {
          break;
        }
        i++;
      }
      if (i == start) {
        throw new IllegalArgumentException("Empty term");
      }
    }

    private void readQuoted() {
      i++;
      while (i < query.length()) {
        char c = query.charAt(i);
        if (c == '\\') {
          i += i + 1 < query.length() ? 2 : 1;
          continue;
        }
        i++;
        if (c == '"') {
          return;
        }
      }
      throw new IllegalArgumentException("Unclosed quote");
    }

    private void readRegex() {
      i++;
      while (i < query.length()) {
        char c = query.charAt(i);
        if (c == '\\') {
          i += i + 1 < query.length() ? 2 : 1;
          continue;
        }
        i++;
        if (c == '/') {
          while (i < query.length() && Character.isLetter(query.charAt(i))) {
            i++;
          }
          return;
        }
      }
      throw new IllegalArgumentException("Unclosed regex");
    }

    private void readBalanced(char open, char close) {
      if (i >= query.length() || query.charAt(i) != open) {
        throw new IllegalArgumentException("Expected " + open);
      }
      i++;
      int groupDepth = 1;
      boolean quoted = false;
      while (i < query.length()) {
        char c = query.charAt(i);
        if (c == '\\') {
          i += i + 1 < query.length() ? 2 : 1;
          continue;
        }
        if (c == '"') {
          quoted = !quoted;
          i++;
          continue;
        }
        if (!quoted && c == open) {
          groupDepth++;
        } else if (!quoted && c == close) {
          groupDepth--;
          i++;
          if (groupDepth == 0) {
            return;
          }
          continue;
        }
        i++;
      }
      throw new IllegalArgumentException("Unbalanced " + open);
    }

    private void readFuzzy() {
      if (i >= query.length() || query.charAt(i) != '~') {
        return;
      }
      i++;
      while (i < query.length()) {
        char c = query.charAt(i);
        if ((c >= '0' && c <= '9') || c == '.') {
          i++;
        } else {
          break;
        }
      }
    }

    /** Consumes a trailing {@code ^boost} when this call is not capturing it into a node. */
    private float readBoost() {
      if (i >= query.length() || query.charAt(i) != '^') {
        return 1f;
      }
      int start = ++i;
      while (i < query.length()) {
        char c = query.charAt(i);
        if ((c >= '0' && c <= '9') || c == '.') {
          i++;
        } else {
          break;
        }
      }
      if (i == start) {
        throw new IllegalArgumentException("Missing boost");
      }
      try {
        return Float.parseFloat(query.substring(start, i));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid boost");
      }
    }

    private boolean startsClause() {
      if (i >= query.length()) {
        return false;
      }
      char c = query.charAt(i);
      if (c == ')' || c == ']' || c == '}') {
        return false;
      }
      if (c == '&' || c == '|') {
        return false;
      }
      return true;
    }

    private boolean lookingAt(String token) {
      return query.startsWith(token, i);
    }

    private boolean lookingAtWord(String word) {
      if (!query.startsWith(word, i)) {
        return false;
      }
      int end = i + word.length();
      return end >= query.length() || isOperatorBoundary(query.charAt(end));
    }

    private boolean consume(String token) {
      if (query.startsWith(token, i)) {
        i += token.length();
        return true;
      }
      return false;
    }

    private boolean consumeWord(String word) {
      if (!lookingAtWord(word)) {
        return false;
      }
      i += word.length();
      return true;
    }

    private void skipWs() {
      while (i < query.length() && isWhitespace(query.charAt(i))) {
        i++;
      }
    }
  }

  private static boolean isWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\u3000';
  }

  private static boolean isFieldChar(char c) {
    if (isWhitespace(c)) {
      return false;
    }
    return "+-!(){}[]^\"~:\\/&|".indexOf(c) < 0;
  }

  private static boolean isOperatorBoundary(char c) {
    return isWhitespace(c) || "+-!(){}[]^\"~:\\/&|".indexOf(c) >= 0;
  }
}
