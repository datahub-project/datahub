package com.linkedin.metadata.search.elasticsearch.query.v3.ranking;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.v3.FieldMapping.Type;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.search.template.RankingBooleanMatch;
import com.linkedin.search.template.RankingBooleanOperator;
import com.linkedin.search.template.RankingBooleanValue;
import com.linkedin.search.template.RankingKeywordMatch;
import com.linkedin.search.template.RankingKeywordPresence;
import com.linkedin.search.template.RankingNormalization;
import com.linkedin.search.template.RankingSignal;
import com.linkedin.search.template.RankingSignalKind;
import com.linkedin.search.template.RankingValueAggregation;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/** Write-time rules for an inline ranking signal list. */
public final class RankingSignalRules {
  private static final Pattern NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private static final Set<String> RESERVED_NAMES =
      Set.of("entityType", MappingConstants.ENTITY_TYPE_FIELD, MappingConstants.SCORE_FIELD);

  private RankingSignalRules() {}

  public static void checkSignals(
      List<RankingSignal> signals,
      SearchFieldResolver resolver,
      @Nullable Collection<EntitySpec> entitySpecs,
      int maxSignals) {
    if (signals == null) {
      return;
    }
    if (signals.size() > maxSignals) {
      throw new SearchTemplateException("Search template ranking has too many signals");
    }
    Set<String> names = new HashSet<>();
    Set<String> entityTypes = entityTypeNames(entitySpecs);
    for (RankingSignal signal : signals) {
      checkSignal(signal, names, resolver, entityTypes);
    }
  }

  public static void checkSignal(
      RankingSignal signal,
      Set<String> names,
      SearchFieldResolver resolver,
      @Nullable Set<String> entityTypes) {
    String name = signal.getName();
    if (name == null || !NAME.matcher(name).matches()) {
      throw new SearchTemplateException("Search template ranking signal name is not valid");
    }
    if (RESERVED_NAMES.contains(name) || !names.add(name)) {
      throw new SearchTemplateException(
          "Search template ranking signal name is not valid: " + name);
    }
    RankingSignalKind kind = signal.getKind();
    if (kind == null) {
      throw new SearchTemplateException("Search template ranking signal kind is required");
    }
    String field = signal.hasField() ? signal.getField() : null;
    boolean property = signal.hasPropertyUrn() && signal.getPropertyUrn() != null;
    switch (kind) {
      case FIELD_NUMBER -> requireField(field, resolver, Intent.RANGE, Type.COUNT, Type.DOUBLE);
      case FIELD_BOOLEAN -> requireField(field, resolver, Intent.FILTER, Type.BOOLEAN);
      case FIELD_KEYWORD -> requireField(field, resolver, Intent.FILTER, Type.KEYWORD);
      case FIELD_AGE_DAYS -> requireField(field, resolver, Intent.RANGE, Type.DATETIME);
      case STRUCTURED_PROPERTY -> {
        if (field != null && !field.isBlank()) {
          throw new SearchTemplateException(
              "Search template structured property signal does not set a field");
        }
        if (!property || !validPropertyUrn(signal.getPropertyUrn())) {
          throw new SearchTemplateException(
              "Search template structured property signal requires a property URN");
        }
      }
      default -> {
        if ((field != null && !field.isBlank()) || property) {
          throw new SearchTemplateException(
              "Search template ranking signal does not take a field: " + kind);
        }
      }
    }
    if (kind != RankingSignalKind.STRUCTURED_PROPERTY && property) {
      throw new SearchTemplateException("Search template ranking signal does not take a property");
    }
    checkValueAggregation(signal, kind);
    checkEntityTypes(signal, entityTypes);
    checkBooleanMatch(signal, kind);
    checkKeywordMatch(signal, kind);
    double boost = signal.getBoost();
    requireOpenClosed("boost", boost, 0, 3);
    checkNormalization(signal.getNormalization(), kind);
    if (kind == RankingSignalKind.TAG_WEIGHTS
        && signal.hasTagWeights()
        && signal.getTagWeights() != null) {
      signal
          .getTagWeights()
          .forEach(
              (key, value) -> {
                if (key == null || !key.startsWith("urn:li:tag:")) {
                  throw new SearchTemplateException(
                      "Search template tag weight key must be a tag URN");
                }
                requireOpenClosed("tag weight", value, 0, 1000);
              });
    }
  }

  private static void checkValueAggregation(RankingSignal signal, RankingSignalKind kind) {
    if (!signal.hasValueAggregation() || signal.getValueAggregation() == null) {
      return;
    }
    if (kind != RankingSignalKind.FIELD_NUMBER
        && kind != RankingSignalKind.FIELD_AGE_DAYS
        && kind != RankingSignalKind.STRUCTURED_PROPERTY) {
      throw new SearchTemplateException(
          "Search template ranking valueAggregation requires a numeric signal");
    }
    if (signal.getValueAggregation() == RankingValueAggregation.$UNKNOWN) {
      throw new SearchTemplateException(
          "Search template ranking valueAggregation is not valid: " + signal.getValueAggregation());
    }
    if (kind == RankingSignalKind.FIELD_AGE_DAYS
        && signal.getValueAggregation() != RankingValueAggregation.MAX
        && signal.getValueAggregation() != RankingValueAggregation.MIN) {
      throw new SearchTemplateException(
          "Search template ranking age valueAggregation must be MAX or MIN");
    }
  }

  private static void checkEntityTypes(RankingSignal signal, @Nullable Set<String> registryTypes) {
    Set<String> included =
        namedTypes(
            signal.hasEntityTypes() ? signal.getEntityTypes() : null, "entityTypes", registryTypes);
    Set<String> excluded =
        namedTypes(
            signal.hasExcludedEntityTypes() ? signal.getExcludedEntityTypes() : null,
            "excludedEntityTypes",
            registryTypes);
    if (included == null || excluded == null) {
      return;
    }
    for (String type : included) {
      if (excluded.contains(type)) {
        throw new SearchTemplateException(
            "Search template ranking names "
                + type
                + " in both entityTypes and excludedEntityTypes");
      }
    }
  }

  private static void checkBooleanMatch(RankingSignal signal, RankingSignalKind kind) {
    if (!signal.hasBooleanMatch() || signal.getBooleanMatch() == null) {
      return;
    }
    if (kind != RankingSignalKind.FIELD_BOOLEAN) {
      throw new SearchTemplateException(
          "Search template ranking booleanMatch requires a boolean signal");
    }
    RankingBooleanMatch match = signal.getBooleanMatch();
    if (match.hasOperator()
        && (match.getOperator() == null
            || match.getOperator() == RankingBooleanOperator.$UNKNOWN)) {
      throw new SearchTemplateException(
          "Search template ranking booleanMatch operator is not valid");
    }
    if (!match.hasValues() || match.getValues() == null || match.getValues().isEmpty()) {
      throw new SearchTemplateException("Search template ranking booleanMatch requires a value");
    }
    Set<RankingBooleanValue> seen = new HashSet<>();
    for (RankingBooleanValue value : match.getValues()) {
      if (value == null || value == RankingBooleanValue.$UNKNOWN) {
        throw new SearchTemplateException(
            "Search template ranking booleanMatch value is not valid: " + value);
      }
      if (!seen.add(value)) {
        throw new SearchTemplateException(
            "Search template ranking booleanMatch names " + value + " more than once");
      }
    }
  }

  private static void checkKeywordMatch(RankingSignal signal, RankingSignalKind kind) {
    boolean present = signal.hasKeywordMatch() && signal.getKeywordMatch() != null;
    if (kind != RankingSignalKind.FIELD_KEYWORD) {
      if (present) {
        throw new SearchTemplateException(
            "Search template ranking keywordMatch requires a keyword signal");
      }
      return;
    }
    if (!present) {
      throw new SearchTemplateException("Search template ranking keywordMatch is required");
    }
    RankingKeywordMatch match = signal.getKeywordMatch();
    if (match.hasOperator()
        && (match.getOperator() == null
            || match.getOperator() == RankingBooleanOperator.$UNKNOWN)) {
      throw new SearchTemplateException(
          "Search template ranking keywordMatch operator is not valid");
    }
    if (match.hasPresence()
        && (match.getPresence() == null
            || match.getPresence() == RankingKeywordPresence.$UNKNOWN)) {
      throw new SearchTemplateException(
          "Search template ranking keywordMatch presence is not valid");
    }
    Set<String> included =
        namedTokens(match.hasValues() ? match.getValues() : null, "keywordMatch values");
    Set<String> excluded =
        namedTokens(
            match.hasExcludedValues() ? match.getExcludedValues() : null,
            "keywordMatch excludedValues");
    boolean hasPresence =
        match.hasPresence()
            && match.getPresence() != null
            && match.getPresence() != RankingKeywordPresence.$UNKNOWN;
    if (included == null && excluded == null && !hasPresence) {
      throw new SearchTemplateException("Search template ranking keywordMatch requires a value");
    }
    if (included != null && excluded != null) {
      for (String token : included) {
        if (excluded.contains(token)) {
          throw new SearchTemplateException(
              "Search template ranking keywordMatch names " + token + " in both lists");
        }
      }
    }
    if (match.hasOperator()
        && match.getOperator() == RankingBooleanOperator.AND
        && match.getPresence() == RankingKeywordPresence.NOT_EXISTS
        && (included != null || excluded != null)) {
      throw new SearchTemplateException(
          "Search template ranking keywordMatch cannot combine NOT_EXISTS with a token list using AND");
    }
  }

  @Nullable
  private static Set<String> namedTokens(@Nullable Iterable<String> tokens, String field) {
    if (tokens == null) {
      return null;
    }
    Set<String> seen = new HashSet<>();
    boolean any = false;
    for (String token : tokens) {
      any = true;
      if (token == null || token.isBlank()) {
        throw new SearchTemplateException("Search template ranking " + field + " requires a value");
      }
      if (!seen.add(token)) {
        throw new SearchTemplateException(
            "Search template ranking " + field + " names " + token + " more than once");
      }
    }
    if (!any) {
      throw new SearchTemplateException("Search template ranking " + field + " requires a value");
    }
    return seen;
  }

  @Nullable
  private static Set<String> namedTypes(
      @Nullable Iterable<String> types, String field, @Nullable Set<String> registryTypes) {
    if (types == null) {
      return null;
    }
    Set<String> seen = new HashSet<>();
    boolean any = false;
    for (String type : types) {
      any = true;
      if (type == null || type.isBlank()) {
        throw new SearchTemplateException(
            "Search template ranking " + field + " requires an entity type");
      }
      if (!seen.add(type)) {
        throw new SearchTemplateException(
            "Search template ranking " + field + " names " + type + " more than once");
      }
      if (registryTypes != null && !registryTypes.contains(type)) {
        throw new SearchTemplateException(
            "Search template ranking " + field + " is not valid: " + type);
      }
    }
    if (!any) {
      throw new SearchTemplateException(
          "Search template ranking " + field + " requires an entity type");
    }
    return seen;
  }

  @Nullable
  private static Set<String> entityTypeNames(@Nullable Collection<EntitySpec> entitySpecs) {
    if (entitySpecs == null) {
      return null;
    }
    Set<String> names = new HashSet<>();
    for (EntitySpec spec : entitySpecs) {
      if (spec != null && spec.getName() != null) {
        names.add(spec.getName());
      }
    }
    return names;
  }

  public static void checkWindow(Integer window, int maxWindow) {
    if (window == null) {
      return;
    }
    if (window <= 0 || window > maxWindow) {
      throw new SearchTemplateException(
          "Search template ranking window must be from 1 to " + maxWindow);
    }
  }

  private static void requireField(
      String field, SearchFieldResolver resolver, Intent intent, Type... allowedTypes) {
    if (field == null) {
      throw new SearchTemplateException("Search template ranking field is not valid: " + field);
    }
    if (resolver == null) {
      return;
    }
    try {
      SearchFieldResolver.Field resolved = resolver.resolve(field, intent);
      if (!field.equals(resolved.getPath())
          || java.util.Arrays.stream(allowedTypes)
              .noneMatch(type -> type == resolved.getRootType())) {
        throw new SearchTemplateException("Search template ranking field is not valid: " + field);
      }
    } catch (IllegalArgumentException e) {
      throw new SearchTemplateException("Search template ranking field is not valid: " + field, e);
    }
  }

  private static boolean validPropertyUrn(Urn urn) {
    return urn != null && "structuredProperty".equals(urn.getEntityType());
  }

  private static void checkNormalization(RankingNormalization norm, RankingSignalKind kind) {
    if (norm == null || norm.getType() == null) {
      throw new SearchTemplateException("Search template ranking normalization is required");
    }
    Double outputMin = norm.hasOutputMin() ? norm.getOutputMin() : null;
    Double outputMax = norm.hasOutputMax() ? norm.getOutputMax() : null;
    if (outputMin != null) {
      requireFinite("outputMin", outputMin);
      if (outputMin <= 0) {
        throw new SearchTemplateException("Search template ranking outputMin must be positive");
      }
    }
    if (outputMax != null) {
      requireFinite("outputMax", outputMax);
    }
    if (outputMin != null && outputMax != null && outputMax < outputMin) {
      throw new SearchTemplateException(
          "Search template ranking outputMax must be at least outputMin");
    }
    Double inputMin = norm.hasInputMin() ? norm.getInputMin() : null;
    Double inputMax = norm.hasInputMax() ? norm.getInputMax() : null;
    if (inputMin != null) {
      requireFinite("inputMin", inputMin);
    }
    if (inputMax != null) {
      requireFinite("inputMax", inputMax);
    }
    if (inputMin != null && inputMax != null && inputMax <= inputMin) {
      throw new SearchTemplateException("Search template ranking inputMax must exceed inputMin");
    }
    if (norm.hasSteepness() && norm.getSteepness() != null) {
      requireFinite("steepness", norm.getSteepness());
    }
    if (norm.hasScale() && norm.getScale() != null) {
      requireFinite("scale", norm.getScale());
      if (norm.getScale() <= 0) {
        throw new SearchTemplateException("Search template ranking scale must be positive");
      }
    }
    if (norm.getType().name().equals("BOOLEAN") || kind == RankingSignalKind.TAG_WEIGHTS) {
      if (norm.hasTrueValue() && norm.getTrueValue() != null) {
        requireOpenClosed("trueValue", norm.getTrueValue(), 0, 1000);
      }
      if (norm.hasFalseValue() && norm.getFalseValue() != null) {
        requireOpenClosed("falseValue", norm.getFalseValue(), 0, 1000);
      }
    }
  }

  private static void requireFinite(String name, double value) {
    if (!Double.isFinite(value)) {
      throw new SearchTemplateException("Search template ranking " + name + " must be finite");
    }
  }

  private static void requireOpenClosed(String name, double value, double min, double max) {
    requireFinite(name, value);
    if (value <= min || value > max) {
      throw new SearchTemplateException(
          "Search template ranking "
              + name
              + " must be greater than "
              + min
              + " and at most "
              + max);
    }
  }
}
