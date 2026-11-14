package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX;

import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.utils.ESUtils;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
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
public final class StructuredQueryRouting {

  private static final int MAX_DEPTH = 32;

  /**
   * A date or date math the date mapping parses: {@code now} or an ISO date, an optional {@code
   * ||}, then {@code [+-]<n><unit>} or {@code /<unit>}. A date without {@code ||} takes no math, so
   * a stray suffix is not sent to the engine. The pattern checks shape only. {@link
   * #realCalendarDate} rejects a month, day, or clock the date mapping cannot parse, such as {@code
   * 2024-02-30}.
   */
  private static final Pattern DATE_OR_DATE_MATH =
      Pattern.compile(
          "^(?:now(?:(?:\\|\\|)?(?:(?:[+-]\\d+|/)[yMwdHhms])+)?|"
              + "\\d{4}-\\d{2}-\\d{2}(?:T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?(?:Z|[+-]\\d{2}:?\\d{2})?)?"
              + "(?:\\|\\|(?:(?:[+-]\\d+|/)[yMwdHhms])+)?)$");

  /** The {@code now} or ISO date that may be followed by {@code ||} date math. */
  private static final Pattern DATE_ANCHOR =
      Pattern.compile(
          "^(?:now|\\d{4}-\\d{2}-\\d{2}(?:T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?(?:Z|[+-]\\d{2}:?\\d{2})?)?)$");

  private static final String DATE_UNITS = "yMwdHhms";

  private StructuredQueryRouting() {}

  public static Optional<QueryBuilder> build(
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
      String clauseText = lenient ? text : quotedExactField(text, field, fieldResolver);
      if (!lenient && !isQuotedFieldValue(text)) {
        QueryBuilder contained = containQuery(text, field, shared, fieldResolver);
        if (contained != null) {
          return withBoost(contained, weight);
        }
      }
      return withBoost(clause(clauseText, lenient ? properties : shared, lenient, true), weight);
    }
  }

  /**
   * An unquoted {@code /q} value uses the same {@code CONTAIN} resolution as a contains filter. A
   * text-only label searches its analyzed subfields. A source name whose label differs, such as
   * {@code fieldPaths}, searches the keyword path. A label written as itself, such as {@code name}
   * or {@code tags}, stays as written. {@code v3.lowercase} reads {@code <field>.lowercase}.
   * Without that subfield a simple value is a case-insensitive term, and a single wildcard is
   * queried as written and in lowercase. A numeric or date root is never case-folded: an ordered
   * value is a range and anything else is an exact term. A quoted value stays an exact query. Its
   * field name is still resolved when that name is not the stored path.
   */
  @Nullable
  private static QueryBuilder containQuery(
      @Nonnull String text,
      @Nonnull String field,
      @Nonnull List<SearchFieldConfig> shared,
      @Nullable SearchFieldResolver fieldResolver) {
    if (fieldResolver == null || !fieldResolver.contains(field)) {
      return null;
    }
    // A boolean root does not advertise CONTAIN: a wildcard there is rejected by the engine and
    // fails the whole search. Exact and range values still have to be real typed terms, so they
    // resolve through EXACT, which a boolean does support.
    if (isNumericOrDateField(fieldResolver, field)) {
      SearchFieldResolver.Field typed = fieldResolver.resolve(field, Intent.EXACT);
      String path =
          field.equals(typed.getPath())
              ? typed.getPath()
              : fieldResolver.exactPath(field, Intent.EXACT);
      return numericOrDateQuery(path, typed.getRootType(), text);
    }
    if (!fieldResolver.supports(field, Intent.CONTAIN)) {
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
      // Parsed URN keywords stay on query_string so the normalizer folds the query.
      if (resolved.isIndexed() && !resolved.isParsedKeywordLabel()) {
        String path =
            resolved.isLowercase()
                ? resolved.getPath() + "." + V3SearchFields.LOWERCASE
                : resolved.getPath();
        String value = simpleValue(text);
        if (value != null) {
          if (resolved.isLowercase()) {
            return QueryBuilders.termQuery(path, ESUtils.keywordNormalize(value));
          }
          return QueryBuilders.termQuery(path, value).caseInsensitive(true);
        }
        String pattern = wildcardValue(text);
        if (pattern != null) {
          if (resolved.isLowercase()) {
            return QueryBuilders.wildcardQuery(path, ESUtils.keywordNormalize(pattern));
          }
          return caseFoldedWildcard(path, pattern);
        }
      }
      return null;
    }
    int colon = text.indexOf(':');
    String keywordPath = fieldResolver.exactPath(field, Intent.CONTAIN);
    return clause(keywordPath + text.substring(colon), shared, false, true);
  }

  static boolean isNumericOrDate(@Nonnull FieldMapping.Type type) {
    return type == FieldMapping.Type.BOOLEAN
        || type == FieldMapping.Type.COUNT
        || type == FieldMapping.Type.DATETIME
        || type == FieldMapping.Type.DOUBLE;
  }

  /** Whether an exact match on this field is a numeric or date value rather than text. */
  static boolean isNumericOrDateField(
      @Nullable SearchFieldResolver resolver, @Nonnull String fieldName) {
    return resolver != null
        && resolver.contains(fieldName)
        && resolver.supports(fieldName, Intent.EXACT)
        && isNumericOrDate(resolver.resolve(fieldName, Intent.EXACT).getRootType());
  }

  /**
   * An ordered value ({@code >1700}, {@code <=1700}) is a range. Any other single value is an exact
   * term. The bound and the term are the mapped type: a boolean, a long for a count or a date, or a
   * double. A date that is not a long is passed through when it is a date or date math. A bare
   * {@code *} is an exists query, as it is in {@code query_string}. Any other value that is not
   * that type matches nothing. A value with query-string syntax stays one.
   */
  @Nullable
  private static QueryBuilder numericOrDateQuery(
      @Nonnull String path, @Nonnull FieldMapping.Type type, @Nonnull String text) {
    String value = singleFieldValue(text);
    if (value == null) {
      return null;
    }
    if ("*".equals(value)) {
      return QueryBuilders.existsQuery(path);
    }
    for (String op : List.of(">=", "<=", ">", "<")) {
      if (value.startsWith(op)) {
        Object bound = typedValue(type, value.substring(op.length()).trim());
        if (bound == null) {
          return new MatchNoneQueryBuilder();
        }
        return switch (op) {
          case ">=" -> QueryBuilders.rangeQuery(path).gte(bound);
          case "<=" -> QueryBuilders.rangeQuery(path).lte(bound);
          case ">" -> QueryBuilders.rangeQuery(path).gt(bound);
          default -> QueryBuilders.rangeQuery(path).lt(bound);
        };
      }
    }
    Object typed = typedValue(type, value);
    if (typed == null) {
      return new MatchNoneQueryBuilder();
    }
    return QueryBuilders.termQuery(path, typed);
  }

  /**
   * The query value in the type the mapping stores. A boolean is only {@code true} or {@code
   * false}; {@link Boolean#parseBoolean} would turn every other word into false. A date that is not
   * a long is the original string when it is a date or date math, and nothing otherwise.
   */
  @Nullable
  private static Object typedValue(@Nonnull FieldMapping.Type type, @Nonnull String value) {
    if (value.isEmpty()) {
      return null;
    }
    try {
      return switch (type) {
        case BOOLEAN ->
            value.equalsIgnoreCase("true")
                ? Boolean.TRUE
                : value.equalsIgnoreCase("false") ? Boolean.FALSE : null;
        case COUNT -> Long.parseLong(value);
        case DATETIME -> longOrDate(value);
        case DOUBLE -> Double.parseDouble(value);
        default -> null;
      };
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** A count-shaped date, or a date or date math string the date mapping parses. */
  @Nullable
  private static Object longOrDate(@Nonnull String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return DATE_OR_DATE_MATH.matcher(value).matches() && realCalendarDate(value) ? value : null;
    }
  }

  /**
   * The date or {@code now} anchor is a real calendar value. Date math after {@code ||} is left to
   * the engine; the anchor in front of it still has to be a date the mapping accepts.
   */
  private static boolean realCalendarDate(@Nonnull String value) {
    int math = value.indexOf("||");
    String anchor = math < 0 ? value : value.substring(0, math);
    if (anchor.startsWith("now")) {
      return true;
    }
    int timeAt = anchor.indexOf('T');
    try {
      LocalDate.parse(timeAt < 0 ? anchor : anchor.substring(0, timeAt));
      if (timeAt < 0) {
        return true;
      }
      String clock = anchor.substring(timeAt + 1);
      int zoneAt = offsetStart(clock);
      LocalTime.parse(zoneAt < 0 ? clock : clock.substring(0, zoneAt));
      if (zoneAt >= 0) {
        ZoneOffset.of(clock.substring(zoneAt));
      }
      return true;
    } catch (DateTimeException e) {
      return false;
    }
  }

  private static int offsetStart(@Nonnull String clock) {
    for (int i = 0; i < clock.length(); i++) {
      char c = clock.charAt(i);
      if (c == 'Z' || c == '+' || c == '-') {
        return i;
      }
    }
    return -1;
  }

  /** A single field value with no query-string syntax and no wildcard. */
  @Nullable
  private static String simpleValue(@Nonnull String text) {
    String value = singleFieldValue(text);
    if (value == null || value.indexOf('*') >= 0 || value.indexOf('?') >= 0) {
      return null;
    }
    return value;
  }

  /**
   * A single wildcard ({@code Orders*}). A space, quotes, a group, a {@code ^} boost, or a {@code
   * ~} fuzzy suffix stays a query_string, which is what applies that syntax.
   */
  @Nullable
  private static String wildcardValue(@Nonnull String text) {
    String value = singleFieldValue(text);
    if (value == null || (value.indexOf('*') < 0 && value.indexOf('?') < 0)) {
      return null;
    }
    return value;
  }

  @Nullable
  private static String singleFieldValue(@Nonnull String text) {
    int colon = text.indexOf(':');
    if (colon < 0 || colon == text.length() - 1) {
      return null;
    }
    String value = text.substring(colon + 1).trim();
    if (value.isEmpty()
        || value.indexOf(' ') >= 0
        || value.indexOf('"') >= 0
        || value.indexOf('(') >= 0
        || value.indexOf(')') >= 0
        || value.indexOf('^') >= 0
        || value.indexOf('~') >= 0) {
      return null;
    }
    return value;
  }

  /**
   * Wildcard on a raw keyword root. The pattern is also queried in lowercase so {@code Orders*}
   * matches a stored {@code orders_fact}. {@code case_insensitive} stays off: on OpenSearch 3 it
   * builds a case-folding automaton that can be null and fails the shard. A field with {@code
   * v3.lowercase} queries {@code <field>.lowercase} once instead.
   */
  @Nonnull
  private static QueryBuilder caseFoldedWildcard(@Nonnull String field, @Nonnull String pattern) {
    QueryBuilder written = QueryBuilders.wildcardQuery(field, pattern);
    String folded = pattern.toLowerCase(Locale.ROOT);
    if (folded.equals(pattern)) {
      return written;
    }
    return QueryBuilders.boolQuery()
        .should(written)
        .should(QueryBuilders.wildcardQuery(field, folded))
        .minimumShouldMatch(1);
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

  /**
   * A quoted value stays an exact query. A name the index does not store as written, such as the V2
   * subfield {@code platform.keyword}, is replaced with the path an exact query reads. The logical
   * field itself, such as {@code platform} or {@code name}, is left as written.
   */
  @Nonnull
  private static String quotedExactField(
      @Nonnull String text, @Nonnull String field, @Nullable SearchFieldResolver fieldResolver) {
    if (!isQuotedFieldValue(text)
        || fieldResolver == null
        || !fieldResolver.contains(field)
        || !fieldResolver.supports(field, Intent.EXACT)) {
      return text;
    }
    SearchFieldResolver.Field resolved = fieldResolver.resolve(field, Intent.EXACT);
    String exact = fieldResolver.exactPath(field, Intent.EXACT);
    if (field.equals(exact) || field.equals(resolved.getPath())) {
      return text;
    }
    int colon = text.indexOf(':');
    return exact + text.substring(colon);
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
          // {@code ||} is OR, except inside a date value, where it introduces date math
          // ({@code 2024-10-01||+1M}). The math stays in the value so it is not a second clause.
          int mathEnd = dateMathEnd(start, i);
          if (mathEnd < 0) {
            break;
          }
          i = mathEnd;
          continue;
        }
        i++;
      }
      if (i == start) {
        throw new IllegalArgumentException("Empty term");
      }
    }

    /**
     * End of a date-math suffix that begins at the {@code ||} at {@code orAt}, or {@code -1} when
     * that {@code ||} is the OR operator. The anchor is the term so far, without a leading range
     * operator.
     */
    private int dateMathEnd(int termStart, int orAt) {
      String anchor = query.substring(termStart, orAt);
      for (String op : List.of(">=", "<=", ">", "<")) {
        if (anchor.startsWith(op)) {
          anchor = anchor.substring(op.length());
          break;
        }
      }
      if (!DATE_ANCHOR.matcher(anchor).matches()) {
        return -1;
      }
      int j = orAt + 2;
      int pieces = 0;
      while (j < query.length()) {
        int next = dateMathPieceEnd(j);
        if (next == j) {
          break;
        }
        j = next;
        pieces++;
      }
      return pieces > 0 ? j : -1;
    }

    /**
     * End of one {@code [+-]<n><unit>} or {@code /<unit>} piece, or {@code j} when there is none.
     */
    private int dateMathPieceEnd(int j) {
      char c = query.charAt(j);
      if (c == '/') {
        return j + 1 < query.length() && DATE_UNITS.indexOf(query.charAt(j + 1)) >= 0 ? j + 2 : j;
      }
      if (c != '+' && c != '-') {
        return j;
      }
      int k = j + 1;
      if (k >= query.length() || !Character.isDigit(query.charAt(k))) {
        return j;
      }
      while (k < query.length() && Character.isDigit(query.charAt(k))) {
        k++;
      }
      return k < query.length() && DATE_UNITS.indexOf(query.charAt(k)) >= 0 ? k + 1 : j;
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
