package com.linkedin.metadata.search.elasticsearch.query.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortBuilder;

/** Parses rendered Elasticsearch JSON and counts leaf clauses. */
public final class SearchTemplateJson {
  private static final NamedXContentRegistry REGISTRY;
  private static final Set<String> FORBIDDEN =
      Set.of("script", "script_score", "wrapper", "percolate");

  static {
    SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
    REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
  }

  private SearchTemplateJson() {}

  @Nonnull
  public static QueryBuilder parseQuery(@Nonnull String json) {
    try (XContentParser parser = parser(foldCaseInsensitiveWildcards(json))) {
      return AbstractQueryBuilder.parseInnerQueryBuilder(parser);
    } catch (IOException | RuntimeException e) {
      throw failure("Search template query is not valid JSON DSL", e);
    }
  }

  /**
   * A wildcard that sets {@code case_insensitive} is rewritten before the engine sees it. Search V3
   * keyword roots keep the stored casing and have no {@code .keyword} subfield. On OpenSearch 3 a
   * case-insensitive wildcard builds a case-folding automaton that can be null and fails the shard.
   * The written pattern stays, and the lowercase pattern is added beside it when they differ. An
   * array clause gets that copy as the next element. An object-valued clause, such as {@code
   * function_score.query} or a single {@code bool.filter}, is wrapped in {@code bool.should}. Term
   * queries keep {@code case_insensitive}.
   */
  @Nonnull
  static String foldCaseInsensitiveWildcards(@Nonnull String json) {
    if (!json.contains("\"case_insensitive\"")) {
      return json;
    }
    JsonNode root = read(json);
    boolean changed = foldWildcards(root);
    if (root instanceof ObjectNode object) {
      List<ObjectNode> extras = new ArrayList<>();
      if (relaxWildcard(object, extras)) {
        changed = true;
        if (!extras.isEmpty()) {
          root = boolShould(object, extras);
        }
      }
    }
    if (!changed) {
      return json;
    }
    try {
      return SearchTemplateMappers.JSON.writeValueAsString(root);
    } catch (IOException e) {
      throw new SearchTemplateException("Search template query is not valid JSON DSL", e);
    }
  }

  private static boolean foldWildcards(@Nonnull JsonNode node) {
    if (node instanceof ObjectNode object) {
      boolean changed = false;
      List<String> names = new ArrayList<>();
      object.fieldNames().forEachRemaining(names::add);
      for (String name : names) {
        JsonNode child = object.get(name);
        if (child instanceof ObjectNode childObject) {
          List<ObjectNode> extras = new ArrayList<>();
          if (relaxWildcard(childObject, extras)) {
            changed = true;
            if (!extras.isEmpty()) {
              object.remove(name);
              object.set(name, boolShould(childObject, extras));
            }
          } else {
            changed |= foldWildcards(childObject);
          }
        } else {
          changed |= foldWildcards(child);
        }
      }
      return changed;
    }
    if (!(node instanceof ArrayNode array)) {
      return false;
    }
    boolean changed = false;
    List<Integer> positions = new ArrayList<>();
    List<ObjectNode> extras = new ArrayList<>();
    for (int i = 0; i < array.size(); i++) {
      JsonNode element = array.get(i);
      changed |= foldWildcards(element);
      List<ObjectNode> folded = new ArrayList<>();
      if (relaxWildcard(element, folded)) {
        changed = true;
        for (int extra = 0; extra < folded.size(); extra++) {
          positions.add(i + 1);
          extras.add(folded.get(extra));
        }
      }
    }
    for (int i = extras.size() - 1; i >= 0; i--) {
      array.insert(positions.get(i), extras.get(i));
    }
    return changed;
  }

  /**
   * Drops {@code case_insensitive} from a one-field wildcard. When the lowercase pattern differs,
   * the caller adds that copy next to the written pattern.
   */
  @Nonnull
  private static ObjectNode boolShould(
      @Nonnull ObjectNode clause, @Nonnull List<ObjectNode> extras) {
    ObjectNode bool = SearchTemplateMappers.JSON.createObjectNode();
    ArrayNode should = bool.putArray("should");
    should.add(clause);
    extras.forEach(should::add);
    bool.put("minimum_should_match", 1);
    ObjectNode wrapped = SearchTemplateMappers.JSON.createObjectNode();
    wrapped.set("bool", bool);
    return wrapped;
  }

  private static boolean relaxWildcard(
      @Nonnull JsonNode element, @Nonnull List<ObjectNode> extras) {
    if (!(element instanceof ObjectNode object)) {
      return false;
    }
    JsonNode wildcard = object.get("wildcard");
    if (!(wildcard instanceof ObjectNode fields) || fields.isEmpty()) {
      return false;
    }
    boolean relaxed = false;
    var clauses = fields.fields();
    while (clauses.hasNext()) {
      JsonNode clause = clauses.next().getValue();
      if (clause instanceof ObjectNode clauseObject
          && clauseObject.path("case_insensitive").asBoolean(false)) {
        clauseObject.remove("case_insensitive");
        relaxed = true;
      }
    }
    if (!relaxed || fields.size() != 1) {
      return relaxed;
    }
    String fieldName = fields.fieldNames().next();
    JsonNode clause = fields.get(fieldName);
    if (!(clause instanceof ObjectNode clauseObject)) {
      return true;
    }
    String valueKey = clauseObject.has("value") ? "value" : "wildcard";
    JsonNode valueNode = clauseObject.get(valueKey);
    if (valueNode == null || !valueNode.isTextual()) {
      return true;
    }
    String folded = valueNode.asText().toLowerCase(Locale.ROOT);
    if (!folded.equals(valueNode.asText())) {
      ObjectNode copy = object.deepCopy();
      ((ObjectNode) copy.get("wildcard").get(fieldName)).put(valueKey, folded);
      extras.add(copy);
    }
    return true;
  }

  @Nonnull
  public static QueryBuilder mergeClauses(
      @Nonnull QueryBuilder query, @Nonnull String clausesJson) {
    JsonNode clauses = read(clausesJson);
    if (!(query instanceof BoolQueryBuilder)) {
      query = QueryBuilders.boolQuery().must(query);
    }
    BoolQueryBuilder bool = (BoolQueryBuilder) query;
    add(bool, "must", clauses.get("must"), bool::must);
    add(bool, "must_not", clauses.get("must_not"), bool::mustNot);
    add(bool, "filter", clauses.get("filter"), bool::filter);
    add(bool, "should", clauses.get("should"), bool::should);
    return bool;
  }

  public record TermsBoost(@Nonnull String field, @Nonnull List<String> values, double weight) {}

  /**
   * Functions and modes applied after the template JSON and list-membership boosts. Factors come
   * before term weights. A null mode leaves the JSON mode in place.
   */
  public record FunctionScoreTail(
      @Nonnull List<JsonNode> functions, @Nullable String scoreMode, @Nullable String boostMode) {
    public FunctionScoreTail {
      functions = List.copyOf(functions);
    }

    @Nonnull
    public static FunctionScoreTail none() {
      return new FunctionScoreTail(List.of(), null, null);
    }

    public boolean applies() {
      return !functions.isEmpty() || scoreMode != null || boostMode != null;
    }
  }

  @Nonnull
  public static ObjectNode fieldFactorFunction(
      @Nonnull String field,
      @Nullable Double factor,
      @Nullable Double missing,
      @Nullable String modifier) {
    ObjectNode function = SearchTemplateMappers.JSON.createObjectNode();
    ObjectNode factorNode = function.putObject("field_value_factor");
    factorNode.put("field", field);
    if (factor != null) {
      factorNode.put("factor", factor);
    }
    if (missing != null) {
      factorNode.put("missing", missing);
    }
    if (modifier != null) {
      factorNode.put("modifier", modifier);
    }
    // field_value_factor fails the shard when its result is negative, and reciprocal also fails
    // on zero. Square stays non-negative for a negative stored value, so it needs no filter.
    if (!"square".equals(modifier)) {
      function.set(
          "filter", nonNegativeField(field, "reciprocal".equals(modifier), missing != null));
    }
    return function;
  }

  /**
   * Documents this function may score. A negative stored value stays out. An absent field is
   * included only when {@code missing} will replace it.
   */
  @Nonnull
  private static ObjectNode nonNegativeField(
      @Nonnull String field, boolean strictlyPositive, boolean includeMissing) {
    ObjectNode range = SearchTemplateMappers.JSON.createObjectNode();
    range.putObject("range").putObject(field).put(strictlyPositive ? "gt" : "gte", 0);
    if (!includeMissing) {
      return range;
    }
    ObjectNode filter = SearchTemplateMappers.JSON.createObjectNode();
    ObjectNode bool = filter.putObject("bool");
    ArrayNode should = bool.putArray("should");
    should.add(range);
    should
        .addObject()
        .putObject("bool")
        .putArray("must_not")
        .addObject()
        .putObject("exists")
        .put("field", field);
    bool.put("minimum_should_match", 1);
    return filter;
  }

  @Nonnull
  public static ObjectNode termWeightFunction(
      @Nonnull String field, @Nonnull JsonNode value, double weight) {
    ObjectNode function = SearchTemplateMappers.JSON.createObjectNode();
    function.putObject("filter").putObject("term").putObject(field).set("value", value);
    function.put("weight", weight);
    return function;
  }

  @Nonnull
  public static QueryBuilder wrapFunctionScore(
      @Nonnull QueryBuilder query,
      @Nullable String functionScoreJson,
      @Nonnull List<TermsBoost> boosts,
      @Nonnull FunctionScoreTail tail) {
    try {
      ObjectNode body =
          functionScoreJson == null || functionScoreJson.isBlank()
              ? defaultFunctionScore()
              : (ObjectNode) SearchTemplateMappers.JSON.readTree(functionScoreJson);
      ArrayNode functions = functions(body);
      for (TermsBoost boost : boosts) {
        if (boost.values().isEmpty()) {
          continue;
        }
        ObjectNode function = functions.addObject();
        ArrayNode values = function.putObject("filter").putObject("terms").putArray(boost.field());
        boost.values().forEach(values::add);
        function.put("weight", boost.weight());
      }
      for (JsonNode extra : tail.functions()) {
        functions.add(extra.deepCopy());
      }
      if (tail.scoreMode() != null) {
        body.put("score_mode", tail.scoreMode());
      }
      if (tail.boostMode() != null) {
        body.put("boost_mode", tail.boostMode());
      }
      body.set("query", SearchTemplateMappers.JSON.readTree(query.toString()));
      String wrapped =
          SearchTemplateMappers.JSON.writeValueAsString(
              SearchTemplateMappers.JSON.createObjectNode().set("function_score", body));
      try (XContentParser parser = parser(wrapped)) {
        QueryBuilder parsed = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
        if (!(parsed instanceof FunctionScoreQueryBuilder)) {
          throw new SearchTemplateException("functionScore did not parse as function_score");
        }
        return parsed;
      }
    } catch (IOException | RuntimeException e) {
      throw failure("Search template functionScore is not valid JSON", e);
    }
  }

  @Nonnull
  private static ObjectNode defaultFunctionScore() {
    ObjectNode body = SearchTemplateMappers.JSON.createObjectNode();
    body.putArray("functions");
    body.put("score_mode", "multiply");
    body.put("boost_mode", "multiply");
    return body;
  }

  @Nonnull
  private static ArrayNode functions(@Nonnull ObjectNode body) {
    JsonNode existing = body.get("functions");
    if (existing == null || existing.isNull()) {
      return body.putArray("functions");
    }
    if (!existing.isArray()) {
      throw new SearchTemplateException("Search template functionScore functions is not an array");
    }
    return (ArrayNode) existing;
  }

  @Nullable
  public static QueryRescorerBuilder parseRescore(@Nullable String rescoreJson) {
    if (rescoreJson == null || rescoreJson.isBlank()) {
      return null;
    }
    try {
      SearchSourceBuilder source = source("{\"rescore\":[" + rescoreJson + "]}");
      if (source.rescores() == null || source.rescores().isEmpty()) {
        return null;
      }
      Object rescorer = source.rescores().get(0);
      if (!(rescorer instanceof QueryRescorerBuilder queryRescorer)) {
        throw new SearchTemplateException("Search template rescore must be a query rescore");
      }
      return queryRescorer;
    } catch (IOException | RuntimeException e) {
      throw failure("Search template rescore is not valid JSON", e);
    }
  }

  @Nullable
  public static List<AggregationBuilder> parseAggregations(@Nullable String aggregationsJson) {
    if (aggregationsJson == null || aggregationsJson.isBlank()) {
      return null;
    }
    try {
      SearchSourceBuilder source = source("{\"aggs\":" + aggregationsJson + "}");
      AggregatorFactories.Builder factories = source.aggregations();
      if (factories == null) {
        return List.of();
      }
      return new ArrayList<>(factories.getAggregatorFactories());
    } catch (IOException | RuntimeException e) {
      throw failure("Search template aggregations are not valid JSON", e);
    }
  }

  @Nullable
  public static List<SortBuilder<?>> parseSort(@Nullable String sortJson) {
    if (sortJson == null || sortJson.isBlank()) {
      return null;
    }
    try {
      SearchSourceBuilder source = source("{\"sort\":" + sortJson + "}");
      List<SortBuilder<?>> sorts = source.sorts();
      return sorts == null ? List.of() : sorts;
    } catch (IOException | RuntimeException e) {
      throw failure("Search template sort is not valid JSON", e);
    }
  }

  public static void clampAggregationSize(@Nonnull AggregationBuilder aggregation, int cap) {
    if (aggregation instanceof TermsAggregationBuilder terms && terms.size() > cap) {
      terms.size(cap);
    }
    for (AggregationBuilder sub : aggregation.getSubAggregations()) {
      clampAggregationSize(sub, cap);
    }
  }

  public static int countLeaves(@Nullable String json) {
    if (json == null || json.isBlank()) {
      return 0;
    }
    return countLeaves(read(json));
  }

  @Nonnull
  public static List<String> forbidden(@Nullable String json) {
    List<String> found = new ArrayList<>();
    if (json == null || json.isBlank()) {
      return found;
    }
    collectForbidden(read(json), found);
    return found;
  }

  public static void rejectForbidden(@Nullable String json, @Nonnull String section) {
    List<String> found = forbidden(json);
    if (!found.isEmpty()) {
      throw new SearchTemplateException("Search template " + section + " contains " + found.get(0));
    }
  }

  public static boolean isTopLevelFunctionScore(@Nullable String queryJson) {
    if (queryJson == null || queryJson.isBlank()) {
      return false;
    }
    JsonNode node = read(queryJson);
    return node.isObject() && node.size() == 1 && node.has("function_score");
  }

  @Nonnull
  public static SearchTemplateKnn parseKnn(@Nonnull String knnJson) {
    JsonNode node = read(knnJson);
    if (!node.isObject() || !node.hasNonNull("field") || node.get("field").asText().isBlank()) {
      throw new SearchTemplateException("Search template knn requires a field");
    }
    int k = node.path("k").asInt(100);
    if (k <= 0) {
      throw new SearchTemplateException("Search template knn.k must be positive");
    }
    long scaled = (long) k * 10L;
    int defaultCandidates = scaled > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) scaled;
    int numCandidates = node.path("num_candidates").asInt(defaultCandidates);
    float keywordWeight = (float) node.path("keyword_weight").asDouble(1.0d);
    float vectorWeight = (float) node.path("vector_weight").asDouble(1.0d);
    return new SearchTemplateKnn(
        node.get("field").asText(), k, numCandidates, keywordWeight, vectorWeight);
  }

  public static boolean boostModeReplace(@Nullable String functionScoreJson) {
    if (functionScoreJson == null || functionScoreJson.isBlank()) {
      return false;
    }
    JsonNode node = read(functionScoreJson);
    return node.has("boost_mode") && "replace".equals(node.get("boost_mode").asText());
  }

  private static int countLeaves(JsonNode node) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return 0;
    }
    if (node.isArray()) {
      int count = 0;
      for (JsonNode child : node) {
        count += countLeaves(child);
      }
      return count;
    }
    if (!node.isObject()) {
      return 0;
    }
    if (node.has("bool")) {
      return countBool(node.get("bool"));
    }
    if (node.has("dis_max")) {
      return countLeaves(node.get("dis_max").path("queries"));
    }
    if (node.has("function_score")) {
      return countFunctionScore(node.get("function_score"));
    }
    if (node.has("constant_score")) {
      JsonNode body = node.get("constant_score");
      return countLeaves(body.has("filter") ? body.get("filter") : body.get("query"));
    }
    if (node.has("boosting")) {
      JsonNode body = node.get("boosting");
      return countLeaves(body.get("positive")) + countLeaves(body.get("negative"));
    }
    if (node.size() == 1 && node.has("terms")) {
      return termsLeaves(node.get("terms"));
    }
    return 1;
  }

  private static int countBool(JsonNode bool) {
    return countLeaves(bool.get("must"))
        + countLeaves(bool.get("must_not"))
        + countLeaves(bool.get("filter"))
        + countLeaves(bool.get("should"));
  }

  private static int countFunctionScore(JsonNode body) {
    int count = countLeaves(body.get("query"));
    JsonNode functions = body.get("functions");
    if (functions != null && functions.isArray()) {
      for (JsonNode function : functions) {
        if (function.has("filter")) {
          count += countLeaves(function.get("filter"));
        }
      }
    }
    return count;
  }

  private static int termsLeaves(JsonNode terms) {
    if (terms == null || !terms.isObject()) {
      return 1;
    }
    for (JsonNode value : terms) {
      if (value.isArray()) {
        return Math.max(1, value.size());
      }
    }
    return 1;
  }

  private static void collectForbidden(JsonNode node, List<String> found) {
    if (node == null || node.isNull()) {
      return;
    }
    if (node.isArray()) {
      node.forEach(child -> collectForbidden(child, found));
      return;
    }
    if (!node.isObject()) {
      return;
    }
    node.fieldNames()
        .forEachRemaining(
            name -> {
              if (FORBIDDEN.contains(name) || "min_score".equals(name)) {
                found.add(name);
              }
              JsonNode child = node.get(name);
              if ("terms".equals(name) && child.isObject()) {
                child
                    .fields()
                    .forEachRemaining(
                        entry -> {
                          JsonNode value = entry.getValue();
                          if (value.isObject() && value.has("index") && value.has("id")) {
                            found.add("terms_lookup");
                          }
                        });
              }
              collectForbidden(child, found);
            });
  }

  @Nonnull
  private static SearchTemplateException failure(@Nonnull String message, @Nonnull Exception e) {
    if (e instanceof SearchTemplateException templateException) {
      return templateException;
    }
    return new SearchTemplateException(message, e);
  }

  @Nonnull
  private static JsonNode read(@Nonnull String json) {
    try {
      return SearchTemplateMappers.JSON.readTree(json);
    } catch (IOException e) {
      throw new SearchTemplateException("Search template section is not valid JSON", e);
    }
  }

  private static void add(
      BoolQueryBuilder bool,
      String name,
      @Nullable JsonNode array,
      Function<QueryBuilder, BoolQueryBuilder> adder) {
    if (array == null || array.isNull()) {
      return;
    }
    if (!array.isArray()) {
      throw new SearchTemplateException("Search template clauses." + name + " must be an array");
    }
    for (JsonNode clause : array) {
      adder.apply(parseQuery(clause.toString()));
    }
  }

  public static int sortClauseCount(@Nullable String sort) {
    if (sort == null) {
      return 0;
    }
    try {
      JsonNode node = SearchTemplateMappers.JSON.readTree(sort);
      if (node.isArray()) {
        return node.size();
      }
      return node.isObject() && !node.isEmpty() ? 1 : 0;
    } catch (Exception e) {
      throw new SearchTemplateException("Search template sort is not valid JSON", e);
    }
  }

  public static int rescoreLeaves(@Nullable String rescore) {
    if (rescore == null) {
      return 0;
    }
    try {
      JsonNode node = SearchTemplateMappers.JSON.readTree(rescore);
      JsonNode rescoreQuery = node.path("query").path("rescore_query");
      if (rescoreQuery.isMissingNode() || rescoreQuery.isNull()) {
        return countLeaves(rescore);
      }
      return countLeaves(rescoreQuery.toString());
    } catch (Exception e) {
      throw new SearchTemplateException("Search template rescore is not valid JSON", e);
    }
  }

  @Nonnull
  private static SearchSourceBuilder source(@Nonnull String json) throws IOException {
    try (XContentParser parser = parser(json)) {
      return SearchSourceBuilder.fromXContent(parser);
    }
  }

  @Nonnull
  private static XContentParser parser(@Nonnull String json) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(REGISTRY, LoggingDeprecationHandler.INSTANCE, json);
  }
}
