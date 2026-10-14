package com.linkedin.metadata.entity.semantic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.IntegerDataSchema;
import com.linkedin.data.schema.LongDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.Counter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup-compiled aspect/path comparison. Disabled and unruled aspects never enter {@link
 * #equivalent}. A ruled aspect is walked once, in place, and unconfigured fields stay strict.
 */
public final class SemanticNoOpComparator {
  public static final String SUPPRESSED_COUNTER = "semantic_noop_suppressed";

  private static final Logger LOG = LoggerFactory.getLogger(SemanticNoOpComparator.class);

  private static final SemanticNoOpComparator DISABLED =
      new SemanticNoOpComparator(Map.of(), new AtomicLong());
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private final Map<String, CompiledAspect> byAspect;
  private final AtomicLong walks;

  private SemanticNoOpComparator(Map<String, CompiledAspect> byAspect, AtomicLong walks) {
    this.byAspect = byAspect;
    this.walks = walks;
  }

  public static SemanticNoOpComparator disabled() {
    return DISABLED;
  }

  /**
   * Parse and validate rules against the entity registry. Invalid configuration fails startup even
   * when comparison is disabled. A rule whose aspect is not in the registry is skipped so a stock
   * list can name aspects a deployment adds later. A disabled or empty rule list does not walk
   * writes.
   */
  public static SemanticNoOpComparator compile(
      boolean enabled,
      @Nullable String rulesJson,
      @Nullable EntityRegistry entityRegistry,
      @Nullable MetricUtils metricUtils) {
    List<Rule> rules = parseAndValidate(rulesJson, entityRegistry);
    if (!enabled || rules.isEmpty()) {
      return DISABLED;
    }
    return new SemanticNoOpComparator(compileAspects(rules, metricUtils), new AtomicLong());
  }

  public boolean hasRules(String aspectName) {
    return byAspect.containsKey(aspectName);
  }

  /** Walks entered by {@link #equivalent}. Unruled aspects do not increment this. */
  public long walkCount() {
    return walks.get();
  }

  /**
   * One in-place comparison. The caller must {@link #hasRules} first so unruled aspects stay on
   * {@code DataTemplateUtil.areEqual}.
   */
  public boolean equivalent(String aspectName, DataMap stored, DataMap incoming) {
    return decide(aspectName, stored, incoming) >= 0;
  }

  /**
   * Same walk as {@link #equivalent}. Returns the mask of strategies that hid a difference, or
   * {@code -1} when the documents are not equivalent. A mask of {@code 0} is equivalent and hid
   * nothing.
   */
  public int decide(String aspectName, DataMap stored, DataMap incoming) {
    CompiledAspect compiled = byAspect.get(aspectName);
    if (compiled == null) {
      return -1;
    }
    walks.incrementAndGet();
    EnumSet<Strategy> suppressingStrategies = EnumSet.noneOf(Strategy.class);
    boolean equal = compareValues(stored, incoming, compiled.root, suppressingStrategies);
    if (!equal) {
      return -1;
    }
    int mask = strategyMask(suppressingStrategies);
    compiled.incrementSuppressed(suppressingStrategies);
    return mask;
  }

  /** Reverses one {@link #decide} increment. Used when a later mutation discards the decision. */
  public void rewindSuppression(String aspectName, int strategyMask) {
    if (strategyMask == 0) {
      return;
    }
    CompiledAspect compiled = byAspect.get(aspectName);
    if (compiled != null) {
      compiled.rewindSuppressed(strategyMask);
    }
  }

  private static int strategyMask(Set<Strategy> strategies) {
    int mask = 0;
    for (Strategy strategy : strategies) {
      mask |= 1 << strategy.ordinal();
    }
    return mask;
  }

  private static boolean compareValues(
      @Nullable Object stored,
      @Nullable Object incoming,
      @Nullable TrieNode node,
      Set<Strategy> suppressingStrategies) {
    if (node != null && node.strategy == Strategy.IGNORE) {
      if (!Objects.equals(stored, incoming)) {
        suppressingStrategies.add(Strategy.IGNORE);
      }
      return true;
    }
    if (stored == incoming) {
      return true;
    }
    if (stored == null || incoming == null) {
      return false;
    }
    if (node != null && node.strategy == Strategy.TIMESTAMP_WINDOW) {
      boolean withinWindow = withinWindow(stored, incoming, node.maxDeltaMs);
      if (withinWindow && !stored.equals(incoming)) {
        suppressingStrategies.add(Strategy.TIMESTAMP_WINDOW);
      }
      return withinWindow;
    }
    if (stored instanceof DataMap && incoming instanceof DataMap) {
      if (node == null || node.fields.isEmpty() && node.wildcard == null) {
        return stored.equals(incoming);
      }
      return compareMaps((DataMap) stored, (DataMap) incoming, node, suppressingStrategies);
    }
    if (stored instanceof DataList && incoming instanceof DataList) {
      if (node == null || (node.wildcard == null && node.strategy != Strategy.UNORDERED)) {
        return stored.equals(incoming);
      }
      return compareLists((DataList) stored, (DataList) incoming, node, suppressingStrategies);
    }
    return stored.equals(incoming);
  }

  private static boolean compareMaps(
      DataMap stored, DataMap incoming, TrieNode node, Set<Strategy> suppressingStrategies) {
    if (!node.ignoreChild) {
      if (stored.size() != incoming.size() || !stored.keySet().equals(incoming.keySet())) {
        return false;
      }
    } else if (!ignoredKeysOnly(stored, incoming, node, suppressingStrategies)
        || !ignoredKeysOnly(incoming, stored, node, suppressingStrategies)) {
      return false;
    }
    for (Map.Entry<String, Object> entry : stored.entrySet()) {
      TrieNode child = node.fields.get(entry.getKey());
      if (child != null && child.strategy == Strategy.IGNORE) {
        if (!Objects.equals(entry.getValue(), incoming.get(entry.getKey()))) {
          suppressingStrategies.add(Strategy.IGNORE);
        }
        continue;
      }
      if (!incoming.containsKey(entry.getKey())) {
        return false;
      }
      if (!compareValues(
          entry.getValue(), incoming.get(entry.getKey()), child, suppressingStrategies)) {
        return false;
      }
    }
    return true;
  }

  /** Keys present on {@code left} and missing on {@code right} must be ignored fields. */
  private static boolean ignoredKeysOnly(
      DataMap left, DataMap right, TrieNode node, Set<Strategy> suppressingStrategies) {
    for (String key : left.keySet()) {
      if (right.containsKey(key)) {
        continue;
      }
      TrieNode child = node.fields.get(key);
      if (child == null || child.strategy != Strategy.IGNORE) {
        return false;
      }
      suppressingStrategies.add(Strategy.IGNORE);
    }
    return true;
  }

  private static boolean compareLists(
      DataList stored, DataList incoming, TrieNode node, Set<Strategy> suppressingStrategies) {
    if (node.strategy != Strategy.UNORDERED) {
      return compareListElements(stored, incoming, node.wildcard, suppressingStrategies, false);
    }
    // Positional success must not share its strategy set with the reorder fallback. A failed
    // index pairing can record a strategy for the wrong element.
    Set<Strategy> positional = EnumSet.noneOf(Strategy.class);
    if (compareListElements(stored, incoming, node.wildcard, positional, false)) {
      suppressingStrategies.addAll(positional);
      return true;
    }
    if (compareListElements(stored, incoming, node.wildcard, suppressingStrategies, true)) {
      suppressingStrategies.add(Strategy.UNORDERED);
      return true;
    }
    return false;
  }

  private static boolean compareListElements(
      DataList stored,
      DataList incoming,
      @Nullable TrieNode element,
      Set<Strategy> suppressingStrategies,
      boolean unordered) {
    int size = stored.size();
    if (size != incoming.size()) {
      return false;
    }
    if (!unordered) {
      for (int i = 0; i < size; i++) {
        if (!compareValues(stored.get(i), incoming.get(i), element, suppressingStrategies)) {
          return false;
        }
      }
      return true;
    }
    String[] storedKeys = new String[size];
    String[] incomingKeys = new String[size];
    Integer[] storedOrder = new Integer[size];
    Integer[] incomingOrder = new Integer[size];
    for (int i = 0; i < size; i++) {
      storedKeys[i] = matchKey(stored.get(i), element);
      incomingKeys[i] = matchKey(incoming.get(i), element);
      storedOrder[i] = i;
      incomingOrder[i] = i;
    }
    Arrays.sort(storedOrder, Comparator.comparing(index -> storedKeys[index]));
    Arrays.sort(incomingOrder, Comparator.comparing(index -> incomingKeys[index]));
    for (int i = 0; i < size; i++) {
      if (!compareValues(
          stored.get(storedOrder[i]),
          incoming.get(incomingOrder[i]),
          element,
          suppressingStrategies)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Sort key that ignores fields a rule already treats as non-identifying, so two array elements
   * that differ only by a windowed timestamp still pair.
   */
  private static String matchKey(@Nullable Object value, @Nullable TrieNode node) {
    StringBuilder key = new StringBuilder();
    appendMatchKey(key, value, node);
    return key.toString();
  }

  private static void appendMatchKey(
      StringBuilder key, @Nullable Object value, @Nullable TrieNode node) {
    if (node != null
        && (node.strategy == Strategy.IGNORE || node.strategy == Strategy.TIMESTAMP_WINDOW)) {
      key.append('*');
      return;
    }
    if (value == null) {
      key.append("null");
      return;
    }
    if (value instanceof DataMap) {
      DataMap map = (DataMap) value;
      key.append('{');
      List<String> names = new ArrayList<>(map.keySet());
      Collections.sort(names);
      for (String name : names) {
        key.append(name).append(':');
        TrieNode child = node == null ? null : node.fields.get(name);
        appendMatchKey(key, map.get(name), child);
        key.append(',');
      }
      key.append('}');
      return;
    }
    if (value instanceof DataList) {
      DataList list = (DataList) value;
      TrieNode element = node == null ? null : node.wildcard;
      boolean unordered = node != null && node.strategy == Strategy.UNORDERED;
      if (!unordered) {
        key.append('[');
        for (Object item : list) {
          appendMatchKey(key, item, element);
          key.append(',');
        }
        key.append(']');
        return;
      }
      List<String> parts = new ArrayList<>(list.size());
      for (Object item : list) {
        parts.add(matchKey(item, element));
      }
      Collections.sort(parts);
      key.append('[');
      for (String part : parts) {
        key.append(part).append(',');
      }
      key.append(']');
      return;
    }
    key.append('"');
    String text = String.valueOf(value);
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '\\' || c == '"') {
        key.append('\\');
      }
      key.append(c);
    }
    key.append('"');
  }

  static boolean withinWindow(Object stored, Object incoming, long maxDeltaMs) {
    if (!(stored instanceof Number) || !(incoming instanceof Number)) {
      return false;
    }
    if (stored instanceof Float
        || stored instanceof Double
        || incoming instanceof Float
        || incoming instanceof Double) {
      return false;
    }
    return withinWindow(((Number) stored).longValue(), ((Number) incoming).longValue(), maxDeltaMs);
  }

  /** {@code abs(stored - incoming) <= maxDeltaMs} without signed overflow. */
  static boolean withinWindow(long stored, long incoming, long maxDeltaMs) {
    if (maxDeltaMs < 0) {
      return false;
    }
    long diff = stored - incoming;
    if (((stored ^ incoming) < 0) && ((stored ^ diff) < 0)) {
      return false;
    }
    if (diff < 0) {
      if (diff == Long.MIN_VALUE) {
        return false;
      }
      diff = -diff;
    }
    return diff <= maxDeltaMs;
  }

  private static Map<String, CompiledAspect> compileAspects(
      List<Rule> rules, @Nullable MetricUtils metricUtils) {
    Map<String, List<Rule>> byAspect = new LinkedHashMap<>();
    for (Rule rule : rules) {
      byAspect.computeIfAbsent(rule.aspect, ignored -> new ArrayList<>()).add(rule);
    }
    Map<String, CompiledAspect> compiled = new LinkedHashMap<>();
    for (Map.Entry<String, List<Rule>> entry : byAspect.entrySet()) {
      MutableNode root = new MutableNode();
      List<Strategy> strategies = new ArrayList<>();
      for (Rule rule : entry.getValue()) {
        insert(root, rule.segments, rule.strategy, rule.maxDeltaMs);
        if (!strategies.contains(rule.strategy)) {
          strategies.add(rule.strategy);
        }
      }
      Map<Strategy, Counter> counters = new EnumMap<>(Strategy.class);
      if (metricUtils != null) {
        for (Strategy strategy : strategies) {
          counters.put(
              strategy,
              metricUtils.registerCounter(
                  SUPPRESSED_COUNTER, "aspect", entry.getKey(), "strategy", strategy.name()));
        }
      }
      compiled.put(entry.getKey(), new CompiledAspect(freeze(root), counters));
    }
    return Collections.unmodifiableMap(compiled);
  }

  private static void insert(
      MutableNode root, List<String> segments, Strategy strategy, long maxDeltaMs) {
    MutableNode current = root;
    for (int i = 0; i < segments.size(); i++) {
      String segment = segments.get(i);
      boolean last = i == segments.size() - 1;
      if ("*".equals(segment)) {
        if (current.wildcard == null) {
          current.wildcard = new MutableNode();
        }
        current = current.wildcard;
      } else {
        current = current.fields.computeIfAbsent(segment, ignored -> new MutableNode());
      }
      if (last) {
        if (current.strategy != null) {
          throw new IllegalArgumentException("Duplicate semantic no-op rule");
        }
        current.strategy = strategy;
        current.maxDeltaMs = maxDeltaMs;
      }
    }
  }

  private static TrieNode freeze(MutableNode node) {
    Map<String, TrieNode> fields = new LinkedHashMap<>();
    boolean ignoreChild = false;
    for (Map.Entry<String, MutableNode> entry : node.fields.entrySet()) {
      TrieNode child = freeze(entry.getValue());
      fields.put(entry.getKey(), child);
      if (child.strategy == Strategy.IGNORE) {
        ignoreChild = true;
      }
    }
    return new TrieNode(
        node.strategy,
        node.maxDeltaMs,
        Collections.unmodifiableMap(fields),
        node.wildcard == null ? null : freeze(node.wildcard),
        ignoreChild);
  }

  private static List<Rule> parseAndValidate(
      @Nullable String rulesJson, @Nullable EntityRegistry entityRegistry) {
    String json = rulesJson == null || rulesJson.isBlank() ? "[]" : rulesJson;
    final RuleJson[] parsed;
    try {
      parsed = MAPPER.readValue(json, RuleJson[].class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("semanticNoOp.rulesJson is not a JSON rule array", e);
    }
    if (parsed == null) {
      throw new IllegalArgumentException("semanticNoOp.rulesJson is not a JSON rule array");
    }
    List<Rule> rules = new ArrayList<>();
    for (RuleJson raw : parsed) {
      if (raw == null
          || raw.aspect == null
          || raw.aspect.isBlank()
          || raw.path == null
          || raw.strategy == null) {
        throw new IllegalArgumentException(
            "semantic no-op rule requires aspect, path, and strategy");
      }
      Strategy strategy;
      try {
        strategy = Strategy.valueOf(raw.strategy);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unknown semantic no-op strategy: " + raw.strategy);
      }
      List<String> segments = pathSegments(raw.path);
      if (strategy == Strategy.TIMESTAMP_WINDOW) {
        if (raw.maxDeltaMs == null || raw.maxDeltaMs < 0) {
          throw new IllegalArgumentException(
              "TIMESTAMP_WINDOW requires a non-negative maxDeltaMs for " + raw.path);
        }
      } else if (raw.maxDeltaMs != null) {
        throw new IllegalArgumentException(strategy + " does not take maxDeltaMs for " + raw.path);
      }
      if (entityRegistry == null) {
        throw new IllegalArgumentException(
            "Entity registry is required to validate semantic no-op rules");
      }
      AspectSpec aspectSpec = entityRegistry.getAspectSpecs().get(raw.aspect);
      if (aspectSpec == null) {
        LOG.warn(
            "Skipping semantic no-op rule for unknown aspect {} path {}", raw.aspect, raw.path);
        continue;
      }
      DataSchema terminal = resolve(aspectSpec.getPegasusSchema(), segments, raw.path);
      if (strategy == Strategy.TIMESTAMP_WINDOW && !isIntegral(terminal)) {
        throw new IllegalArgumentException(
            "TIMESTAMP_WINDOW requires a numeric field, but " + raw.path + " is not integral");
      }
      if (strategy == Strategy.UNORDERED && !(dereference(terminal) instanceof ArrayDataSchema)) {
        throw new IllegalArgumentException(
            "UNORDERED requires an array, but " + raw.path + " is not");
      }
      rules.add(
          new Rule(
              raw.aspect,
              raw.path,
              segments,
              strategy,
              raw.maxDeltaMs == null ? 0L : raw.maxDeltaMs));
    }
    rejectOverlaps(rules);
    return rules;
  }

  private static void rejectOverlaps(List<Rule> rules) {
    for (int i = 0; i < rules.size(); i++) {
      for (int j = i + 1; j < rules.size(); j++) {
        Rule left = rules.get(i);
        Rule right = rules.get(j);
        if (left.aspect.equals(right.aspect) && rulesOverlap(left, right)) {
          throw new IllegalArgumentException(
              "Overlapping semantic no-op rules for "
                  + left.aspect
                  + ": "
                  + left.path
                  + " and "
                  + right.path);
        }
      }
    }
  }

  /**
   * {@link Strategy#UNORDERED} on an array describes how elements pair. Rules on fields inside
   * those elements are not a second treatment of the array itself.
   */
  private static boolean rulesOverlap(Rule left, Rule right) {
    if (!overlaps(left.segments, right.segments)) {
      return false;
    }
    if (left.strategy == Strategy.UNORDERED && isStrictPrefix(left.segments, right.segments)) {
      return false;
    }
    return right.strategy != Strategy.UNORDERED || !isStrictPrefix(right.segments, left.segments);
  }

  private static boolean isStrictPrefix(List<String> prefix, List<String> longer) {
    if (prefix.size() >= longer.size()) {
      return false;
    }
    for (int i = 0; i < prefix.size(); i++) {
      if (!prefix.get(i).equals(longer.get(i))) {
        return false;
      }
    }
    return true;
  }

  static boolean overlaps(List<String> left, List<String> right) {
    int shared = Math.min(left.size(), right.size());
    for (int i = 0; i < shared; i++) {
      String a = left.get(i);
      String b = right.get(i);
      if (!(a.equals(b) || "*".equals(a) || "*".equals(b))) {
        return false;
      }
    }
    return true;
  }

  static List<String> pathSegments(String path) {
    if (path == null || !path.startsWith("/") || path.endsWith("/") || path.contains("//")) {
      throw new IllegalArgumentException("Invalid semantic no-op path: " + path);
    }
    String[] parts = path.substring(1).split("/", -1);
    List<String> segments = new ArrayList<>(parts.length);
    for (String part : parts) {
      if (part.isEmpty() || (part.indexOf('*') >= 0 && !"*".equals(part))) {
        throw new IllegalArgumentException("Invalid semantic no-op path: " + path);
      }
      segments.add(part);
    }
    if (segments.isEmpty()) {
      throw new IllegalArgumentException("Invalid semantic no-op path: " + path);
    }
    return segments;
  }

  private static DataSchema resolve(RecordDataSchema root, List<String> segments, String path) {
    DataSchema current = root;
    for (String segment : segments) {
      current = dereference(current);
      if ("*".equals(segment)) {
        if (!(current instanceof ArrayDataSchema)) {
          throw new IllegalArgumentException("Wildcard in " + path + " is only valid on an array");
        }
        current = ((ArrayDataSchema) current).getItems();
        continue;
      }
      DataSchema next = resolveField(current, segment);
      if (next == null) {
        throw new IllegalArgumentException("Path does not resolve: " + path);
      }
      current = next;
    }
    return dereference(current);
  }

  @Nullable
  private static DataSchema resolveField(DataSchema schema, String name) {
    schema = dereference(schema);
    if (schema instanceof RecordDataSchema) {
      RecordDataSchema.Field field = ((RecordDataSchema) schema).getField(name);
      return field == null ? null : field.getType();
    }
    if (schema instanceof UnionDataSchema) {
      DataSchema found = null;
      for (UnionDataSchema.Member member : ((UnionDataSchema) schema).getMembers()) {
        DataSchema candidate = resolveField(member.getType(), name);
        if (candidate != null) {
          if (found != null) {
            throw new IllegalArgumentException("Path is ambiguous on a union at '" + name + "'");
          }
          found = candidate;
        }
      }
      return found;
    }
    return null;
  }

  private static DataSchema dereference(DataSchema schema) {
    while (schema instanceof TyperefDataSchema) {
      schema = ((TyperefDataSchema) schema).getDereferencedDataSchema();
    }
    return schema;
  }

  private static boolean isIntegral(DataSchema schema) {
    DataSchema dereferenced = dereference(schema);
    return dereferenced instanceof IntegerDataSchema || dereferenced instanceof LongDataSchema;
  }

  private enum Strategy {
    IGNORE,
    TIMESTAMP_WINDOW,
    UNORDERED
  }

  private static final class RuleJson {
    public String aspect;
    public String path;
    public String strategy;
    public Long maxDeltaMs;
  }

  private static final class Rule {
    final String aspect;
    final String path;
    final List<String> segments;
    final Strategy strategy;
    final long maxDeltaMs;

    Rule(String aspect, String path, List<String> segments, Strategy strategy, long maxDeltaMs) {
      this.aspect = aspect;
      this.path = path;
      this.segments = segments;
      this.strategy = strategy;
      this.maxDeltaMs = maxDeltaMs;
    }
  }

  private static final class MutableNode {
    Strategy strategy;
    long maxDeltaMs;
    final Map<String, MutableNode> fields = new LinkedHashMap<>();
    MutableNode wildcard;
  }

  private static final class TrieNode {
    final Strategy strategy;
    final long maxDeltaMs;
    final Map<String, TrieNode> fields;
    final TrieNode wildcard;
    final boolean ignoreChild;

    TrieNode(
        Strategy strategy,
        long maxDeltaMs,
        Map<String, TrieNode> fields,
        TrieNode wildcard,
        boolean ignoreChild) {
      this.strategy = strategy;
      this.maxDeltaMs = maxDeltaMs;
      this.fields = fields;
      this.wildcard = wildcard;
      this.ignoreChild = ignoreChild;
    }
  }

  private static final class CompiledAspect {
    final TrieNode root;
    final Map<Strategy, Counter> counters;

    CompiledAspect(TrieNode root, Map<Strategy, Counter> counters) {
      this.root = root;
      this.counters = counters;
    }

    void incrementSuppressed(Set<Strategy> suppressingStrategies) {
      addSuppressed(suppressingStrategies, 1.0d);
    }

    void rewindSuppressed(int strategyMask) {
      EnumSet<Strategy> strategies = EnumSet.noneOf(Strategy.class);
      for (Strategy strategy : Strategy.values()) {
        if ((strategyMask & (1 << strategy.ordinal())) != 0) {
          strategies.add(strategy);
        }
      }
      addSuppressed(strategies, -1.0d);
    }

    private void addSuppressed(Set<Strategy> strategies, double amount) {
      for (Strategy strategy : strategies) {
        Counter counter = counters.get(strategy);
        if (counter != null) {
          counter.increment(amount);
        }
      }
    }
  }
}
