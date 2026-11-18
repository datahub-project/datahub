package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.embedding.EmbeddingProfile;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.search.template.SearchTemplateProfiles;
import com.linkedin.search.template.SearchTemplateRetrieval;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Bind-time gates for the semantic template shapes.
 *
 * <p>Template level: a shape needs capabilities, and a build that does not advertise one fails the
 * template closed. keywordRerank is the exception: without {@code documentRerank} the rung keeps
 * keyword order, so neither the missing capability nor the profile selection only the rerank uses
 * fails it.
 *
 * <p>Entity level: a requested entity type joins the vector leg only when this build runs it for
 * the resolved profile class, it is in {@code enabledEntities}, and its resolved embedding profile
 * covers it on this deployment's system. An entity that fails is left out of the vector leg, or
 * keeps its keyword place under keywordRerank.
 */
public final class SearchTemplateGates {
  public static final String DOCUMENT_RERANK = "documentRerank";
  public static final String VECTOR_RETRIEVAL = "vectorRetrieval";
  public static final String SIMILARITY = "similarity";
  public static final String ENTITY_PARTITION = "entityPartition";
  public static final String VECTOR_THEN_KEYWORD = "vectorThenKeyword";
  public static final String VECTOR_FILTER = "vectorFilter";
  public static final String SEVERAL_PROFILES = "severalProfiles";
  public static final String VECTOR_BODY = "vectorBody";
  public static final String NATIVE_MIXED_QUERY = "nativeMixedQuery";

  /** Capability names a shape can require. */
  public static final Set<String> CAPABILITIES =
      Set.of(
          DOCUMENT_RERANK,
          VECTOR_RETRIEVAL,
          SIMILARITY,
          ENTITY_PARTITION,
          VECTOR_THEN_KEYWORD,
          VECTOR_FILTER,
          SEVERAL_PROFILES,
          VECTOR_BODY,
          NATIVE_MIXED_QUERY);

  /**
   * Capabilities this build runs. The pull request that executes a shape advertises its name, so a
   * build never binds a shape it cannot run. {@code vectorRetrieval} is {@code combine: vector}
   * only.
   */
  public static final Set<String> ADVERTISED = Set.of(DOCUMENT_RERANK, VECTOR_RETRIEVAL);

  /** Entity types this build runs for each embedding profile class. */
  public static final Map<String, Set<String>> ADVERTISED_ENTITY_TYPES =
      Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of(DOCUMENT_ENTITY_NAME));

  /** A keyword rung selects profiles it never queries. */
  public static final String KEYWORD_PROFILES = "profiles";

  /** A named profile does not cover every scoped entity type. */
  public static final String PROFILE_SCOPE = "profileScope";

  /** {@code minSimilarity} is a raw cosine floor and a resolved profile is not cosine. */
  public static final String MIN_SIMILARITY = "minSimilarity";

  /** This build does not run the entity type for the resolved profile class. */
  public static final String ENTITY_NOT_ADVERTISED = "entityNotAdvertised";

  /** The entity type is not in {@code semanticSearch.enabledEntities}. */
  public static final String ENTITY_NOT_ENABLED = "entityNotEnabled";

  private static final ObjectMapper JSON = new ObjectMapper();

  private final EmbeddingProfileRegistry profiles;
  @Nullable private final SemanticSearchConfiguration semanticSearch;
  private final boolean hybridRead;
  private final Set<String> advertised;
  private final Map<String, Set<String>> advertisedEntityTypes;

  /**
   * @param semanticSearch the deployment's semantic search settings, for {@code enabledEntities}
   * @param hybridRead this edition's hybrid read flag is on. keywordRerank reranks only with it.
   */
  public SearchTemplateGates(
      @Nonnull EmbeddingProfileRegistry profiles,
      @Nullable SemanticSearchConfiguration semanticSearch,
      boolean hybridRead,
      @Nonnull Set<String> advertised,
      @Nonnull Map<String, Set<String>> advertisedEntityTypes) {
    for (String name : advertised) {
      if (!CAPABILITIES.contains(name)) {
        throw new IllegalArgumentException("Unknown search template capability " + name);
      }
    }
    this.profiles = profiles;
    this.semanticSearch = semanticSearch;
    this.hybridRead = hybridRead;
    this.advertised = Set.copyOf(advertised);
    this.advertisedEntityTypes = Map.copyOf(advertisedEntityTypes);
  }

  /** This build's gates over the deployment's registry, semantic search, and hybrid read. */
  @Nonnull
  public static SearchTemplateGates of(
      @Nonnull EmbeddingProfileRegistry profiles,
      @Nullable SemanticSearchConfiguration semanticSearch,
      boolean hybridRead) {
    return new SearchTemplateGates(
        profiles, semanticSearch, hybridRead, ADVERTISED, ADVERTISED_ENTITY_TYPES);
  }

  /** This build's gates with no registered profile, semantic search off, and hybrid read off. */
  @Nonnull
  public static SearchTemplateGates none() {
    return of(EmbeddingProfileRegistry.empty(), null, false);
  }

  @Nonnull
  public EmbeddingProfileRegistry profiles() {
    return profiles;
  }

  public boolean advertises(@Nonnull String capability) {
    return advertised.contains(capability);
  }

  /** This edition's hybrid read flag. */
  public boolean hybridRead() {
    return hybridRead;
  }

  /** Capabilities the merged template's shape needs. */
  @Nonnull
  public static Set<String> required(@Nonnull MergedSearchTemplate template) {
    Set<String> needs = new LinkedHashSet<>();
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    String combine = SearchTemplateCapabilities.effectiveCombine(template);
    if (combine != null) {
      needs.add(
          switch (combine) {
            case SearchTemplateCapabilities.COMBINE_KEYWORD_RERANK -> DOCUMENT_RERANK;
            case SearchTemplateCapabilities.COMBINE_VECTOR -> VECTOR_RETRIEVAL;
            case VECTOR_THEN_KEYWORD -> VECTOR_THEN_KEYWORD;
            case VECTOR_FILTER -> VECTOR_FILTER;
            // Write rejects any other value; a stored one still fails closed under its own name.
            default -> combine;
          });
    }
    if (retrieval != null
        && SearchTemplateCapabilities.QUERY_SOURCE_ANCHOR_ENTITY.equals(
            retrieval.getQuerySource())) {
      needs.add(SIMILARITY);
    }
    if (template.getScope() != null) {
      needs.add(ENTITY_PARTITION);
    }
    if (retrieval != null && StringUtils.isNotBlank(retrieval.getVectorBody())) {
      needs.add(VECTOR_BODY);
    }
    if (SearchTemplateCapabilities.citesEmbeddingPlaceholders(template)) {
      needs.add(NATIVE_MIXED_QUERY);
    }
    // How the resolved set combines several profiles. The resolved set itself is counted at bind,
    // because a named profile outside an entity's types falls back to that entity's default.
    if (retrieval != null && StringUtils.isNotBlank(retrieval.getProfileScore())) {
      needs.add(SEVERAL_PROFILES);
    }
    return needs;
  }

  /**
   * Bounded reason the template fails closed in this build, from its shape alone, or null: a
   * required capability this build does not advertise, a keyword rung that selects profiles, or a
   * named profile that does not cover every scoped entity type.
   *
   * @param documentRerank this request can run document rerank (semantic read on and the request
   *     includes Document). keywordRerank also needs this edition's hybrid read flag.
   */
  @Nullable
  public String rejectionCode(@Nonnull MergedSearchTemplate template, boolean documentRerank) {
    Set<String> needs = required(template);
    boolean selectionUnused = needs.contains(DOCUMENT_RERANK) && !reranks(documentRerank);
    if (selectionUnused) {
      // keywordRerank keeps keyword order when this request cannot rerank, and only the rerank uses
      // the profile selection, so neither fails the search.
      needs.remove(DOCUMENT_RERANK);
      needs.remove(SEVERAL_PROFILES);
    }
    for (String need : needs) {
      if (!advertises(need)) {
        return need;
      }
    }
    // A keyword rung has no vector leg. A native mixed query is not a keyword rung: its
    // placeholders use the selection.
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    if (SearchTemplateCapabilities.effectiveCombine(template) == null
        && !needs.contains(NATIVE_MIXED_QUERY)
        && (template.getProfiles() != null
            || (retrieval != null && StringUtils.isNotBlank(retrieval.getProfileScore())))) {
      return KEYWORD_PROFILES;
    }
    return selectionUnused ? null : scopeRejection(template);
  }

  /**
   * Template failure that depends on the requested entity types, or null. Over the entities that
   * join the vector leg (enabled, and run by this build for their profile class): the selection
   * names more than one profile, even when one of them is unavailable here, or the template's own
   * {@code minSimilarity} meets a covering profile that is not cosine. A floor inherited from
   * configuration is skipped on such a profile instead. Only a vector leg this build runs for this
   * request is checked. An entity that fails coverage is reported by {@link #entityGates}.
   */
  @Nullable
  public String bindingRejection(
      @Nonnull MergedSearchTemplate template,
      @Nullable Collection<String> entityTypes,
      @Nullable String system,
      boolean documentRerank) {
    if (entityTypes == null || !runsVectorLeg(template, documentRerank)) {
      return null;
    }
    SearchTemplateProfiles selection = selection(template);
    Set<String> selected = new LinkedHashSet<>();
    Set<String> covering = new LinkedHashSet<>();
    for (String entityType : members(template, entityTypes)) {
      EmbeddingProfileRegistry.Resolution resolution =
          profiles.resolve(selection, entityType, system);
      if (!countsToward(resolution, entityType)) {
        continue;
      }
      selected.addAll(resolution.profileIds());
      if (resolution.covered()) {
        covering.addAll(resolution.profileIds());
      }
    }
    if (selected.size() > 1 && !advertises(SEVERAL_PROFILES)) {
      return SEVERAL_PROFILES;
    }
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    if (retrieval != null && retrieval.hasMinSimilarity()) {
      for (String id : covering) {
        EmbeddingProfile profile = profiles.profile(id);
        if (profile != null && !profile.cosine()) {
          return MIN_SIMILARITY;
        }
      }
    }
    return null;
  }

  /**
   * The raw cosine floor a rung applies: {@code retrieval.minSimilarity}, else, on a combine that
   * admits vector hits, the deployment's {@code semanticSearch.minSimilarity}. Null is no floor.
   */
  @Nullable
  public Float minSimilarity(@Nonnull MergedSearchTemplate template) {
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    if (retrieval != null && retrieval.hasMinSimilarity()) {
      return retrieval.getMinSimilarity();
    }
    return semanticSearch != null && SearchTemplateCapabilities.admitsVectorHits(template)
        ? semanticSearch.getMinSimilarity()
        : null;
  }

  /**
   * Entity-level gate for each requested type the template covers, in request order: null when the
   * entity joins the vector leg, else the bounded reason it is left out.
   */
  @Nonnull
  public Map<String, String> entityGates(
      @Nonnull MergedSearchTemplate template,
      @Nonnull Collection<String> entityTypes,
      @Nullable String system) {
    SearchTemplateProfiles selection = selection(template);
    Map<String, String> gates = new LinkedHashMap<>();
    for (String entityType : members(template, entityTypes)) {
      EmbeddingProfileRegistry.Resolution resolution =
          profiles.resolve(selection, entityType, system);
      String reason = joinGate(resolution, entityType);
      gates.put(entityType, reason == null ? resolution.failure() : reason);
    }
    return Collections.unmodifiableMap(gates);
  }

  /**
   * Profiles the vector leg resolves for the requested types that pass the entity gates, in request
   * order.
   */
  @Nonnull
  public Set<String> joinedProfileIds(
      @Nonnull MergedSearchTemplate template,
      @Nonnull Collection<String> entityTypes,
      @Nullable String system) {
    SearchTemplateProfiles selection = selection(template);
    Set<String> ids = new LinkedHashSet<>();
    for (String entityType : members(template, entityTypes)) {
      EmbeddingProfileRegistry.Resolution resolution =
          profiles.resolve(selection, entityType, system);
      if (joinGate(resolution, entityType) == null && resolution.failure() == null) {
        ids.addAll(resolution.profileIds());
      }
    }
    return ids;
  }

  /**
   * The profiles selection the vector leg resolves. {@code profiles} wins. Otherwise a semantic
   * shape that names an embedding through the deprecated {@code model} or {@code field} selects
   * that id, with no alias. A legacy knn with no combine keeps its field as opaque syntax and
   * resolves to the entity default.
   */
  @Nullable
  static SearchTemplateProfiles selection(@Nonnull MergedSearchTemplate template) {
    if (template.getProfiles() != null || template.isProfilesSet()) {
      return template.getProfiles();
    }
    String named = legacyProfileId(template);
    return named == null
        ? null
        : new SearchTemplateProfiles().setReplace(new StringArray(List.of(named)));
  }

  /**
   * The profile id a semantic shape names through the deprecated {@code knn} or {@code retrieval}
   * {@code model} and {@code field}, or null. Rejects names that disagree. Any explicit combine
   * reads them.
   */
  @Nullable
  public static String legacyProfileId(@Nonnull MergedSearchTemplate template) {
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    String combine = SearchTemplateCapabilities.combine(retrieval);
    if (combine == null) {
      return null;
    }
    Set<String> names = new LinkedHashSet<>();
    addName(names, retrieval.getModel());
    addName(names, retrieval.getField());
    JsonNode knn = readKnn(template.getKnn());
    if (knn != null) {
      addName(names, knn.path("model").isTextual() ? knn.path("model").asText() : null);
      addName(names, knn.path("field").isTextual() ? knn.path("field").asText() : null);
    }
    if (names.size() > 1) {
      throw new SearchTemplateException(
          "Search template model and field name different embedding profiles: " + names);
    }
    return names.isEmpty() ? null : names.iterator().next();
  }

  // A combine or a native mixed query has a vector leg. It runs when this build advertises every
  // capability the shape needs and, for keywordRerank, this request can rerank.
  private boolean runsVectorLeg(@Nonnull MergedSearchTemplate template, boolean documentRerank) {
    Set<String> needs = required(template);
    if (SearchTemplateCapabilities.effectiveCombine(template) == null
        && !needs.contains(NATIVE_MIXED_QUERY)) {
      return false;
    }
    if (needs.contains(DOCUMENT_RERANK) && !reranks(documentRerank)) {
      return false;
    }
    for (String need : needs) {
      if (!advertises(need)) {
        return false;
      }
    }
    return true;
  }

  // keywordRerank reranks only with hybrid read on, the same flag that gates today's reranker.
  private boolean reranks(boolean documentRerank) {
    return documentRerank && hybridRead && advertises(DOCUMENT_RERANK);
  }

  @Nullable
  private String scopeRejection(@Nonnull MergedSearchTemplate template) {
    if (template.getScope() == null || template.getScope().getEntityTypes() == null) {
      return null;
    }
    for (String id : named(selection(template))) {
      EmbeddingProfile profile = profiles.profile(id);
      for (String entityType : template.getScope().getEntityTypes()) {
        if (profile == null || !profile.covers(entityType)) {
          return PROFILE_SCOPE;
        }
      }
    }
    return null;
  }

  // The gates that decide whether an entity can join the vector leg at all, before coverage: it is
  // enabled, and this build runs it for each selected profile's class.
  @Nullable
  private String joinGate(
      @Nonnull EmbeddingProfileRegistry.Resolution resolution, @Nonnull String entityType) {
    if (!SemanticEmbeddingMappings.isEnabledForEntity(semanticSearch, entityType)) {
      return ENTITY_NOT_ENABLED;
    }
    if (!advertisedFor(resolution.profileIds(), entityType)) {
      return ENTITY_NOT_ADVERTISED;
    }
    return null;
  }

  // An enabled entity counts toward the one-profile rule unless every profile it selects that the
  // registry knows is of a class this build does not run for it. A second profile of another
  // class still counts, so the selection fails closed instead of emptying the leg.
  private boolean countsToward(
      @Nonnull EmbeddingProfileRegistry.Resolution resolution, @Nonnull String entityType) {
    if (!SemanticEmbeddingMappings.isEnabledForEntity(semanticSearch, entityType)) {
      return false;
    }
    boolean known = false;
    for (String id : resolution.profileIds()) {
      EmbeddingProfile profile = profiles.profile(id);
      if (profile == null) {
        continue;
      }
      known = true;
      Set<String> types = advertisedEntityTypes.get(profile.profileClass());
      if (types != null && types.contains(entityType)) {
        return true;
      }
    }
    return !known;
  }

  // An unknown id has no class here; coverage reports it.
  private boolean advertisedFor(@Nonnull List<String> profileIds, @Nonnull String entityType) {
    for (String id : profileIds) {
      EmbeddingProfile profile = profiles.profile(id);
      if (profile == null) {
        continue;
      }
      Set<String> types = advertisedEntityTypes.get(profile.profileClass());
      if (types == null || !types.contains(entityType)) {
        return false;
      }
    }
    return true;
  }

  @Nonnull
  private static List<String> members(
      @Nonnull MergedSearchTemplate template, @Nonnull Collection<String> entityTypes) {
    if (template.getScope() == null || template.getScope().getEntityTypes() == null) {
      return List.copyOf(new LinkedHashSet<>(entityTypes));
    }
    List<String> members = new ArrayList<>();
    for (String entityType : new LinkedHashSet<>(entityTypes)) {
      if (template.getScope().getEntityTypes().contains(entityType)) {
        members.add(entityType);
      }
    }
    return members;
  }

  @Nonnull
  private static Set<String> named(@Nullable SearchTemplateProfiles selection) {
    Set<String> ids = new LinkedHashSet<>();
    if (selection == null) {
      return ids;
    }
    if (selection.hasReplace() && selection.getReplace() != null) {
      ids.addAll(selection.getReplace());
    }
    if (selection.hasAdd() && selection.getAdd() != null) {
      ids.addAll(selection.getAdd());
    }
    return ids;
  }

  private static void addName(@Nonnull Set<String> names, @Nullable String name) {
    if (StringUtils.isNotBlank(name)) {
      names.add(name);
    }
  }

  // Rendered with the write-time sample context, as the write check renders it, so Mustache tags
  // elsewhere in the section do not hide the names. A section that does not render to an object
  // names no id; write rejects it.
  @Nullable
  static JsonNode readKnn(@Nullable String knn) {
    if (knn == null || knn.isBlank()) {
      return null;
    }
    try {
      String rendered =
          SearchTemplateMustache.render(knn, SearchTemplateMustache.validationContext(knn));
      JsonNode node = rendered == null ? null : JSON.readTree(rendered);
      return node != null && node.isObject() ? node : null;
    } catch (Exception e) {
      return null;
    }
  }
}
