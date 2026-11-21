package com.linkedin.metadata.aspect.consistency.check;

import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;

import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.FatalConsistencyException;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields.DocumentValues;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields.ProfileValues;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRefresh;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * On-demand consistency check comparing SQL {@code semanticContent} against Search V3 entity
 * documents when both semantic writes and reads are on V3.
 *
 * <p>Compares only the minimal root and per-profile scalar/marker fields that identify dropped
 * partial writes without reading {@code chunks} arrays or embedding vectors from OpenSearch.
 */
@Slf4j
@Component("consistencySemanticContentIndexMismatchCheck")
public class SemanticContentIndexMismatchCheck implements ConsistencyCheck {

  public static final String CHECK_ID = "semantic-content-index-mismatch";
  public static final String ASPECTS_METADATA_KEY = "aspects";
  private static final String PRIMARY_SEARCH_GROUP = "primary";

  public static class IndexReadFailedException extends FatalConsistencyException {
    public IndexReadFailedException(String message) {
      super(message);
    }

    public IndexReadFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private final SemanticContentIndexFields indexFields;
  private final ElasticSearchConfiguration elasticSearchConfiguration;
  private final EntityDocumentIdHasher documentIdHasher;
  private final List<SemanticContentCheckSkip> checkSkips;
  private final AtomicBoolean disabledLogged = new AtomicBoolean();

  @Autowired
  public SemanticContentIndexMismatchCheck(
      @Nonnull SemanticContentIndexFields indexFields,
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      ObjectProvider<EntityDocumentIdHasher> hasherProvider,
      @Autowired(required = false) @Nullable List<SemanticContentCheckSkip> checkSkips) {
    this(
        indexFields,
        elasticSearchConfiguration,
        hasherProvider.getIfAvailable(Sha256UrnEntityDocumentIdHasher::new),
        checkSkips == null ? List.of() : List.copyOf(checkSkips));
  }

  SemanticContentIndexMismatchCheck(
      @Nonnull SemanticContentIndexFields indexFields,
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      @Nonnull EntityDocumentIdHasher documentIdHasher,
      @Nonnull List<SemanticContentCheckSkip> checkSkips) {
    this.indexFields = Objects.requireNonNull(indexFields, "indexFields");
    this.elasticSearchConfiguration =
        Objects.requireNonNull(elasticSearchConfiguration, "elasticSearchConfiguration");
    this.documentIdHasher = Objects.requireNonNull(documentIdHasher, "documentIdHasher");
    this.checkSkips = List.copyOf(checkSkips);
  }

  @Override
  @Nonnull
  public String getId() {
    return CHECK_ID;
  }

  @Override
  @Nonnull
  public String getName() {
    return "Semantic Content Index Mismatch";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Verifies that Search V3 root and per-profile semanticContent fields match primary storage";
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE;
  }

  @Override
  @Nonnull
  public Optional<Set<String>> getRequiredAspects() {
    return Optional.of(Set.of(SEMANTIC_CONTENT_ASPECT_NAME));
  }

  @Override
  @Nonnull
  public Set<String> getTargetAspects() {
    return Set.of(SEMANTIC_CONTENT_ASPECT_NAME);
  }

  @Override
  public boolean isOnDemandOnly() {
    return true;
  }

  @Override
  @Nonnull
  public List<ConsistencyIssue> check(
      @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
    if (entityResponses.isEmpty()) {
      return List.of();
    }
    EntityIndexConfiguration entityIndex = elasticSearchConfiguration.getEntityIndex();
    SemanticSearchConfiguration semanticSearch =
        entityIndex == null ? null : entityIndex.getSemanticSearch();
    if (!SemanticEntitySearchService.shouldReadSemanticV3(entityIndex)
        || !SemanticEmbeddingMappings.enabledForAnyEntity(semanticSearch)) {
      logDisabled();
      return List.of();
    }

    OperationContext opContext = ctx.getOperationContext();
    String entityType = entityResponses.keySet().iterator().next().getEntityType();
    EntitySpec entitySpec;
    try {
      entitySpec = opContext.getEntityRegistry().getEntitySpec(entityType);
    } catch (RuntimeException e) {
      return List.of();
    }
    if (!entitySpec.hasAspect(SEMANTIC_CONTENT_ASPECT_NAME)
        || !isEntityEligible(entitySpec, semanticSearch)) {
      return List.of();
    }

    Set<Urn> orphanUrns = ctx.getOrphanUrns(entityType);
    Map<Urn, SemanticContent> sqlByUrn = new LinkedHashMap<>();
    Map<Urn, String> docIdsByUrn = new LinkedHashMap<>();
    for (Map.Entry<Urn, EntityResponse> entry : entityResponses.entrySet()) {
      Urn urn = entry.getKey();
      if (orphanUrns.contains(urn) || shouldSkip(urn)) {
        continue;
      }
      SemanticContent sql = aspectOf(entry.getValue());
      if (sql == null) {
        continue;
      }
      sqlByUrn.put(urn, sql);
      docIdsByUrn.put(urn, documentIdHasher.documentId(opContext, urn));
    }
    if (sqlByUrn.isEmpty()) {
      return List.of();
    }

    Map<String, Integer> expectedDimensions = expectedVectorDimensions(semanticSearch);
    String indexName =
        EntitySearchIndexResolver.indexName(opContext, entitySpec.getName(), entityIndex);
    Map<String, DocumentValues> docsById;
    try {
      docsById =
          indexFields.byDocumentId(
              opContext, indexName, docIdsByUrn.values(), expectedDimensions.keySet());
    } catch (Exception e) {
      throw new IndexReadFailedException(
          "Failed to read semanticContent fields on " + indexName, e);
    }

    List<ConsistencyIssue> issues = new ArrayList<>();
    for (Map.Entry<Urn, SemanticContent> entry : sqlByUrn.entrySet()) {
      Urn urn = entry.getKey();
      DocumentValues doc = docsById.get(docIdsByUrn.get(urn));
      compareEntity(urn, entityType, indexName, entry.getValue(), doc, expectedDimensions)
          .ifPresent(issues::add);
    }
    return issues;
  }

  @Nonnull
  private Optional<ConsistencyIssue> compareEntity(
      @Nonnull Urn urn,
      @Nonnull String entityType,
      @Nonnull String indexName,
      @Nonnull SemanticContent sql,
      @Nullable DocumentValues doc,
      @Nonnull Map<String, Integer> expectedDimensions) {
    if (doc == null) {
      return Optional.of(
          ConsistencyIssue.forEntity(urn, entityType, CHECK_ID, ConsistencyFixType.RESTORE_INDICES)
              .description("Search V3 document is missing for entity with semanticContent")
              .details("index=" + indexName + ", reason=missing_document")
              .build());
    }
    if (doc.tombstone()) {
      return Optional.of(
          ConsistencyIssue.forEntity(urn, entityType, CHECK_ID, ConsistencyFixType.RESTORE_INDICES)
              .description("Search V3 document is tombstoned for live entity with semanticContent")
              .details("index=" + indexName + ", reason=tombstone_document")
              .build());
    }

    List<String> mismatches = new ArrayList<>();
    if (!Objects.equals(sql.getSkipReason(GetMode.NULL), doc.skipReason())) {
      mismatches.add("skipReason");
    }

    SemanticRefreshMarker sqlMarker = sql.getRefreshMarker(GetMode.NULL);
    if (sqlMarker == null) {
      if (doc.refreshMarker() != null) {
        mismatches.add("refreshMarker");
      }
    } else if (!markerCovers(doc.refreshMarker(), sqlMarker)) {
      mismatches.add("refreshMarker");
    }

    Map<String, EmbeddingModelData> validSqlProfiles = validSqlProfiles(sql, expectedDimensions);
    for (String modelKey : expectedDimensions.keySet()) {
      EmbeddingModelData sqlProfile = validSqlProfiles.get(modelKey);
      ProfileValues v3Profile = doc.profiles().get(modelKey);
      String prefix = "embeddings." + modelKey;
      if (sqlProfile == null) {
        if (v3Profile != null) {
          mismatches.add(prefix + ":unexpected_profile");
        }
        continue;
      }
      if (v3Profile == null) {
        mismatches.add(prefix + ":missing_profile");
        continue;
      }
      compareProfile(prefix, sqlProfile, v3Profile, mismatches);
    }

    if (mismatches.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        ConsistencyIssue.forEntity(urn, entityType, CHECK_ID, ConsistencyFixType.RESTORE_INDICES)
            .description("Search V3 semanticContent fields differ from primary storage")
            .details("index=" + indexName + ", mismatches=" + String.join(",", mismatches))
            .metadata(Map.of(ASPECTS_METADATA_KEY, SEMANTIC_CONTENT_ASPECT_NAME))
            .build());
  }

  private static void compareProfile(
      @Nonnull String prefix,
      @Nonnull EmbeddingModelData sqlProfile,
      @Nonnull ProfileValues v3Profile,
      @Nonnull List<String> mismatches) {
    if (!Objects.equals(sqlProfile.getTotalChunks(GetMode.NULL), v3Profile.totalChunks())) {
      mismatches.add(prefix + ".totalChunks");
    }
    if (!Objects.equals(sqlProfile.getGeneratedAt(GetMode.NULL), v3Profile.generatedAt())) {
      mismatches.add(prefix + ".generatedAt");
    }
    SemanticVectorState sqlVectorState = sqlProfile.getVectorState(GetMode.NULL);
    if (sqlVectorState != null && !Objects.equals(sqlVectorState.name(), v3Profile.vectorState())) {
      mismatches.add(prefix + ".vectorState");
    }
    Boolean sqlCurrent = sqlProfile.isCurrent(GetMode.NULL);
    if (sqlCurrent != null && !Objects.equals(sqlCurrent, v3Profile.current())) {
      mismatches.add(prefix + ".current");
    }
    String sqlContentHash = sqlProfile.getContentHash(GetMode.NULL);
    if (sqlContentHash != null && !Objects.equals(sqlContentHash, v3Profile.contentHash())) {
      mismatches.add(prefix + ".contentHash");
    }
    String sqlSourceSha = sqlProfile.getSourceTextSha256(GetMode.NULL);
    if (sqlSourceSha != null && !Objects.equals(sqlSourceSha, v3Profile.sourceTextSha256())) {
      mismatches.add(prefix + ".sourceTextSha256");
    }
    SemanticRefreshMarker sqlProfileMarker = sqlProfile.getRefreshMarker(GetMode.NULL);
    if (sqlProfileMarker != null && !markerCovers(v3Profile.refreshMarker(), sqlProfileMarker)) {
      mismatches.add(prefix + ".refreshMarker");
    }
  }

  static boolean markerCovers(
      @Nullable SemanticRefreshMarker actual, @Nonnull SemanticRefreshMarker expected) {
    if (actual == null) {
      return false;
    }
    for (Map.Entry<String, SemanticSourceToken> entry : expected.getSources().entrySet()) {
      SemanticSourceToken actualToken = actual.getSources().get(entry.getKey());
      if (actualToken == null
          || SemanticProfileRefresh.compare(actualToken, entry.getValue()) < 0) {
        return false;
      }
    }
    return true;
  }

  @Nonnull
  private static Map<String, EmbeddingModelData> validSqlProfiles(
      @Nonnull SemanticContent sql, @Nonnull Map<String, Integer> expectedDimensions) {
    if (!sql.hasEmbeddings() || sql.getEmbeddings().isEmpty()) {
      return Map.of();
    }
    Map<String, EmbeddingModelData> valid = new LinkedHashMap<>();
    sql.getEmbeddings()
        .forEach(
            (modelKey, profile) -> {
              if (profile != null
                  && expectedDimensions.containsKey(modelKey)
                  && isValidSqlProfile(profile, expectedDimensions.get(modelKey))) {
                valid.put(modelKey, profile);
              }
            });
    return valid;
  }

  static boolean isValidSqlProfile(
      @Nonnull EmbeddingModelData profile, @Nullable Integer expectedDimension) {
    if (expectedDimension == null || expectedDimension <= 0 || !profile.hasChunks()) {
      return false;
    }
    for (EmbeddingChunk chunk : profile.getChunks()) {
      if (chunk == null || !chunk.hasVector() || chunk.getVector().size() != expectedDimension) {
        return false;
      }
    }
    return true;
  }

  @Nonnull
  private static Map<String, Integer> expectedVectorDimensions(
      @Nonnull SemanticSearchConfiguration semanticSearch) {
    Map<String, ModelEmbeddingConfig> models = semanticSearch.getModels();
    if (models == null || models.isEmpty()) {
      return Map.of();
    }
    Map<String, Integer> dimensions = new LinkedHashMap<>();
    models.forEach(
        (modelKey, modelConfig) -> {
          if (modelConfig != null) {
            dimensions.put(modelKey, modelConfig.getVectorDimension());
          }
        });
    return dimensions;
  }

  private static boolean isEntityEligible(
      @Nonnull EntitySpec entitySpec, @Nonnull SemanticSearchConfiguration semanticSearch) {
    return PRIMARY_SEARCH_GROUP.equals(V3IndexKeys.resolve(entitySpec))
        || SemanticEmbeddingMappings.isEnabledForEntity(semanticSearch, entitySpec.getName());
  }

  private boolean shouldSkip(@Nonnull Urn urn) {
    for (SemanticContentCheckSkip skip : checkSkips) {
      if (skip.skip(urn)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  private static SemanticContent aspectOf(@Nullable EntityResponse response) {
    if (!aspectPresent(response)) {
      return null;
    }
    Aspect value = response.getAspects().get(SEMANTIC_CONTENT_ASPECT_NAME).getValue();
    return new SemanticContent(value.data());
  }

  public static boolean aspectPresent(@Nullable EntityResponse response) {
    if (response == null || response.getAspects() == null) {
      return false;
    }
    EnvelopedAspect aspect = response.getAspects().get(SEMANTIC_CONTENT_ASPECT_NAME);
    return aspect != null && aspect.getValue() != null && aspect.getValue().data() != null;
  }

  private void logDisabled() {
    if (disabledLogged.compareAndSet(false, true)) {
      log.info(
          "semantic-content-index-mismatch skipped: Search V3 semantic read/write is not enabled");
    }
  }
}
