package com.linkedin.metadata.aspect.consistency.check;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields.DocumentValues;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields.Page;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyMappingCache;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.StructuredPropertyUrnIdentity;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.structured.StructuredProperties;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
 * On-demand check that mapped structured-property fields equal the current SQL aspect.
 *
 * <p>The system-metadata page covers entities whose aspect still exists. {@link #scanIndexes}
 * covers the other direction: a document that still has {@code structuredProperties} after the
 * aspect row is gone. A URN-filtered run uses {@link #lookupUrns} for that case, because a full
 * index scroll would include documents the caller did not request. Documents with no
 * structured-property value are not read. The mapping cache is the same one a RESTATE uses; when
 * that feature is off this check does not read mappings.
 */
@Slf4j
@Component("consistencyStructuredPropertyIndexMismatchCheck")
public class StructuredPropertyIndexMismatchCheck implements ConsistencyCheck {

  public static final String CHECK_ID = "structured-property-index-mismatch";

  /** Scroll ids with this prefix continue the index pass instead of the SQL pass. */
  public static final String INDEX_SCROLL_PREFIX = "spidx:";

  /**
   * A mapping, document, or projection read failed. The SQL cursor must not advance: the index pass
   * does not compare entities whose aspect is still present.
   */
  public static final class IndexReadFailedException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public IndexReadFailedException(String message) {
      super(message);
    }

    public IndexReadFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final String TARGETS = "targets";
  private static final String DONE = "done";
  private static final String CURSOR = "cursor";

  private final StructuredPropertyMappingCache mappingCache;
  private final StructuredPropertyIndexFields indexFields;
  private final SearchDocumentTransformer transformer;
  private final ElasticSearchConfiguration elasticSearchConfiguration;
  private final EntityService<?> entityService;
  private final EntityDocumentIdHasher documentIdHasher;
  private final ObjectMapper objectMapper;
  private final AuditStamp projectionStamp;
  private final AtomicBoolean disabledLogged = new AtomicBoolean();

  @Autowired
  public StructuredPropertyIndexMismatchCheck(
      @Nonnull StructuredPropertyMappingCache mappingCache,
      @Nonnull StructuredPropertyIndexFields indexFields,
      @Nonnull SearchDocumentTransformer transformer,
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      @Nonnull EntityService<?> entityService,
      ObjectProvider<EntityDocumentIdHasher> hasherProvider) {
    this(
        mappingCache,
        indexFields,
        transformer,
        elasticSearchConfiguration,
        entityService,
        hasherProvider.getIfAvailable(Sha256UrnEntityDocumentIdHasher::new),
        new ObjectMapper());
  }

  public StructuredPropertyIndexMismatchCheck(
      @Nonnull StructuredPropertyMappingCache mappingCache,
      @Nonnull StructuredPropertyIndexFields indexFields,
      @Nonnull SearchDocumentTransformer transformer,
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      @Nonnull EntityService<?> entityService,
      @Nonnull EntityDocumentIdHasher documentIdHasher,
      @Nonnull ObjectMapper objectMapper) {
    this.mappingCache = mappingCache;
    this.indexFields = indexFields;
    this.transformer = transformer;
    this.elasticSearchConfiguration = elasticSearchConfiguration;
    this.entityService = entityService;
    this.documentIdHasher = documentIdHasher;
    this.objectMapper = objectMapper;
    this.projectionStamp = new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR));
  }

  /** Mapped fields to write, and mapped fields to remove, for one index. */
  public record FieldChange(
      @Nonnull Map<String, JsonNode> values,
      @Nonnull List<String> clear,
      int missing,
      int different,
      int extra) {
    public boolean isEmpty() {
      return values.isEmpty() && clear.isEmpty();
    }
  }

  /** One index whose mapped fields differ, or whose mapping or document could not be read. */
  public record TargetDiff(
      @Nonnull String indexName,
      @Nonnull String documentId,
      @Nullable FieldChange change,
      @Nullable String error) {}

  /** One page of the index pass. {@code scrollId} is null when every target index is done. */
  public record IndexScan(
      @Nonnull List<ConsistencyIssue> issues, @Nullable String scrollId, int documents) {}

  record IndexTarget(@Nonnull String indexName, boolean v3) {}

  @Override
  @Nonnull
  public String getId() {
    return CHECK_ID;
  }

  @Override
  @Nonnull
  public String getName() {
    return "Structured Property Index Mismatch";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Mapped structured-property fields on each enabled search index equal the current SQL"
        + " structuredProperties aspect";
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE;
  }

  @Override
  @Nonnull
  public Optional<Set<String>> getRequiredAspects() {
    return Optional.of(Set.of(STRUCTURED_PROPERTIES_ASPECT_NAME));
  }

  @Override
  @Nonnull
  public Set<String> getTargetAspects() {
    return Set.of(STRUCTURED_PROPERTIES_ASPECT_NAME);
  }

  @Override
  public boolean isOnDemandOnly() {
    return true;
  }

  public boolean mappingReadsEnabled() {
    return mappingCache.isEnabled();
  }

  public static boolean isIndexContinuation(@Nullable String scrollId) {
    return scrollId != null && scrollId.startsWith(INDEX_SCROLL_PREFIX);
  }

  @Override
  @Nonnull
  public List<ConsistencyIssue> check(
      @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
    if (entityResponses.isEmpty()) {
      return List.of();
    }
    if (!mappingCache.isEnabled()) {
      logDisabled();
      return List.of();
    }
    OperationContext opContext = ctx.getOperationContext();
    String entityType = entityResponses.keySet().iterator().next().getEntityType();
    // A missing aspect is not compared here. The index pass reports mapped fields left behind
    // after the row is gone, and reporting them on both passes emits the same issue twice.
    List<Urn> withAspect = new ArrayList<>();
    for (Map.Entry<Urn, EntityResponse> entry : entityResponses.entrySet()) {
      if (aspectPresent(entry.getValue())) {
        withAspect.add(entry.getKey());
      }
    }
    if (withAspect.isEmpty()) {
      return List.of();
    }
    List<IndexTarget> targets = targets(opContext, entityType);
    Map<String, Set<String>> mappedByIndex = new LinkedHashMap<>();
    Map<String, Map<String, DocumentValues>> documentsByIndex = new LinkedHashMap<>();
    for (IndexTarget target : targets) {
      Optional<Set<String>> mapped = mappingCache.fields(opContext, target.indexName());
      if (mapped.isEmpty()) {
        throw new IndexReadFailedException(
            "Failed to read structured property mapping for " + target.indexName());
      }
      if (mapped.get().isEmpty()) {
        continue;
      }
      mappedByIndex.put(target.indexName(), mapped.get());
      List<String> documentIds = new ArrayList<>(withAspect.size());
      for (Urn urn : withAspect) {
        documentIds.add(documentId(opContext, urn, target));
      }
      try {
        documentsByIndex.put(
            target.indexName(),
            indexFields.byDocumentId(opContext, target.indexName(), documentIds));
      } catch (Exception e) {
        throw new IndexReadFailedException(
            "Failed to read structured properties on " + target.indexName(), e);
      }
    }

    List<ConsistencyIssue> issues = new ArrayList<>();
    for (Urn urn : withAspect) {
      StructuredProperties aspect = aspectOf(entityResponses.get(urn));
      Map<String, FieldChange> changes = new LinkedHashMap<>();
      for (IndexTarget target : targets) {
        Set<String> mapped = mappedByIndex.get(target.indexName());
        if (mapped == null) {
          continue;
        }
        Optional<Map<String, JsonNode>> expected = project(opContext, urn, aspect, target, mapped);
        if (expected.isEmpty()) {
          throw new IndexReadFailedException(
              "Failed to project structured properties for " + urn + " on " + target.indexName());
        }
        DocumentValues document =
            documentsByIndex
                .getOrDefault(target.indexName(), Map.of())
                .get(documentId(opContext, urn, target));
        FieldChange change =
            diff(expected.get(), document == null ? null : document.fields(), mapped);
        if (!change.isEmpty()) {
          changes.put(target.indexName(), change);
        }
      }
      issueFor(urn, entityType, changes).ifPresent(issues::add);
    }
    return issues;
  }

  /**
   * One page of documents that still have {@code structuredProperties}. Entities whose SQL aspect
   * is still present are left to {@link #check}; this pass reports mapped fields that remain after
   * the aspect is gone.
   */
  @Nonnull
  public IndexScan scanIndexes(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      int batchSize,
      @Nullable String scrollToken) {
    if (!mappingCache.isEnabled()) {
      logDisabled();
      return new IndexScan(List.of(), null, 0);
    }
    List<IndexTarget> targets = targets(opContext, entityType);
    if (targets.isEmpty()) {
      return new IndexScan(List.of(), null, 0);
    }
    // An unreadable token must fail the page. Returning a finished scan would publish no cursor
    // and leave the remaining indexes unread.
    ScanState state = scrollToken == null ? new ScanState() : decode(scrollToken);
    while (true) {
      IndexTarget target = nextTarget(targets, state);
      if (target == null) {
        return new IndexScan(List.of(), null, 0);
      }
      Optional<Set<String>> mapped = mappingCache.fields(opContext, target.indexName());
      if (mapped.isEmpty()) {
        // A failed read is not an empty catalog. Marking the index done would skip every
        // leftover document and let the run finish with no cursor.
        throw new IllegalStateException(
            "Failed to read structured property mapping for " + target.indexName());
      }
      if (mapped.get().isEmpty()) {
        finish(state, target.indexName());
        continue;
      }
      String cursor = state.cursors.get(target.indexName());
      Page page;
      try {
        page =
            indexFields.scroll(
                opContext, target.indexName(), batchSize, cursor, target.v3() ? entityType : null);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to scroll structured properties on " + target.indexName(), e);
      }
      String next = page.scrollId();
      if (page.documents().isEmpty()) {
        advance(state, target.indexName(), cursor, next);
        continue;
      }
      // Advance only after SQL succeeds. A failed read must not publish this cursor: the page
      // would be skipped for the rest of the run, including fields left behind after a delete.
      List<ConsistencyIssue> issues =
          issuesForIndexPage(opContext, entityType, target, mapped.get(), page.documents());
      advance(state, target.indexName(), cursor, next);
      boolean more = nextTarget(targets, state) != null;
      return new IndexScan(issues, more ? encode(state) : null, page.documents().size());
    }
  }

  /**
   * Point lookup for a URN-filtered run. The caller passes URNs whose aspect was not compared on
   * the SQL pages. A scroll hit whose aspect is already gone is included: that page did not report
   * its leftover fields. The index scroll is not used: it would report leftover fields on URNs the
   * caller did not ask for. An absent document, or one with no mapped structured-property value, is
   * not an issue. An aspect that is still present was compared on the SQL pages. A failed mapping
   * or index read fails the page so the run is retried.
   */
  @Nonnull
  public IndexScan lookupUrns(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull Collection<Urn> urns) {
    if (!mappingCache.isEnabled()) {
      logDisabled();
      return new IndexScan(List.of(), null, 0);
    }
    List<Urn> requested = new ArrayList<>();
    for (Urn urn : urns) {
      if (entityType.equals(urn.getEntityType())) {
        requested.add(urn);
      }
    }
    if (requested.isEmpty()) {
      return new IndexScan(List.of(), null, 0);
    }
    List<IndexTarget> targets = targets(opContext, entityType);
    if (targets.isEmpty()) {
      return new IndexScan(List.of(), null, 0);
    }

    int documents = 0;
    Map<Urn, List<IndexedHit>> hitsByUrn = new LinkedHashMap<>();
    for (IndexTarget target : targets) {
      Optional<Set<String>> mapped = mappingCache.fields(opContext, target.indexName());
      if (mapped.isEmpty()) {
        throw new IllegalStateException(
            "Failed to read structured property mapping for " + target.indexName());
      }
      if (mapped.get().isEmpty()) {
        continue;
      }
      List<String> documentIds = new ArrayList<>(requested.size());
      Map<String, Urn> urnByDocumentId = new LinkedHashMap<>();
      for (Urn urn : requested) {
        String documentId = documentId(opContext, urn, target);
        documentIds.add(documentId);
        urnByDocumentId.put(documentId, urn);
      }
      Map<String, DocumentValues> found;
      try {
        found = indexFields.byDocumentId(opContext, target.indexName(), documentIds);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to read structured properties on " + target.indexName(), e);
      }
      documents += found.size();
      for (DocumentValues document : found.values()) {
        if (document.fields().isEmpty()) {
          continue;
        }
        Urn urn = urnByDocumentId.get(document.documentId());
        if (urn == null) {
          continue;
        }
        hitsByUrn
            .computeIfAbsent(urn, ignored -> new ArrayList<>())
            .add(new IndexedHit(target.indexName(), mapped.get(), document));
      }
    }
    if (hitsByUrn.isEmpty()) {
      return new IndexScan(List.of(), null, documents);
    }

    Map<Urn, EntityResponse> responses;
    try {
      responses =
          entityService.getEntitiesV2(
              opContext,
              entityType,
              hitsByUrn.keySet(),
              Set.of(STRUCTURED_PROPERTIES_ASPECT_NAME),
              false);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load structured properties from SQL for " + entityType, e);
    }

    List<ConsistencyIssue> issues = new ArrayList<>();
    for (Map.Entry<Urn, List<IndexedHit>> entry : hitsByUrn.entrySet()) {
      if (aspectPresent(responses.get(entry.getKey()))) {
        continue;
      }
      Map<String, FieldChange> changes = new LinkedHashMap<>();
      for (IndexedHit hit : entry.getValue()) {
        FieldChange change = diff(Map.of(), hit.document().fields(), hit.mapped());
        if (!change.isEmpty()) {
          changes.put(hit.indexName(), change);
        }
      }
      issueFor(entry.getKey(), entityType, changes).ifPresent(issues::add);
    }
    return new IndexScan(issues, null, documents);
  }

  private record IndexedHit(
      @Nonnull String indexName, @Nonnull Set<String> mapped, @Nonnull DocumentValues document) {}

  /**
   * Current SQL projection compared with a fresh index read. An empty change is omitted. A read
   * that fails is reported on that index and does not stop the others. An absent document is an
   * error, not a field write: the caller must not create a stub.
   */
  @Nonnull
  public List<TargetDiff> recompute(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (!mappingCache.isEnabled()) {
      return List.of();
    }
    StructuredProperties aspect = loadAspect(opContext, urn);
    List<TargetDiff> diffs = new ArrayList<>();
    for (IndexTarget target : targets(opContext, urn.getEntityType())) {
      String docId = documentId(opContext, urn, target);
      Optional<Set<String>> mapped = mappingCache.fields(opContext, target.indexName());
      if (mapped.isEmpty()) {
        diffs.add(new TargetDiff(target.indexName(), docId, null, "mapping could not be read"));
        continue;
      }
      if (mapped.get().isEmpty()) {
        continue;
      }
      Optional<Map<String, JsonNode>> expected =
          project(opContext, urn, aspect, target, mapped.get());
      if (expected.isEmpty()) {
        diffs.add(
            new TargetDiff(
                target.indexName(), docId, null, "structured properties could not be projected"));
        continue;
      }
      DocumentValues document;
      try {
        document =
            indexFields.byDocumentId(opContext, target.indexName(), List.of(docId)).get(docId);
      } catch (Exception e) {
        diffs.add(new TargetDiff(target.indexName(), docId, null, e.getMessage()));
        continue;
      }
      if (document == null) {
        if (!expected.get().isEmpty()) {
          diffs.add(
              new TargetDiff(
                  target.indexName(),
                  docId,
                  null,
                  "search document is absent; a full index write is required"));
        }
        continue;
      }
      FieldChange change = diff(expected.get(), document.fields(), mapped.get());
      if (!change.isEmpty()) {
        diffs.add(new TargetDiff(target.indexName(), docId, change, null));
      }
    }
    return diffs;
  }

  @Nonnull
  private List<ConsistencyIssue> issuesForIndexPage(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull IndexTarget target,
      @Nonnull Set<String> mapped,
      @Nonnull List<DocumentValues> documents) {
    Map<Urn, DocumentValues> byUrn = new LinkedHashMap<>();
    for (DocumentValues document : documents) {
      if (document.urn() == null || document.urn().isEmpty()) {
        continue;
      }
      try {
        Urn urn = UrnUtils.getUrn(document.urn());
        if (entityType.equals(urn.getEntityType())) {
          byUrn.put(urn, document);
        }
      } catch (Exception e) {
        log.debug("Skipping structured property document {} with no URN", document.documentId());
      }
    }
    if (byUrn.isEmpty()) {
      return List.of();
    }
    Map<Urn, EntityResponse> responses;
    try {
      responses =
          entityService.getEntitiesV2(
              opContext,
              entityType,
              byUrn.keySet(),
              Set.of(STRUCTURED_PROPERTIES_ASPECT_NAME),
              false);
    } catch (Exception e) {
      // A failed read cannot prove the aspect is gone, and it must not look like an empty page.
      throw new IllegalStateException(
          "Failed to load structured properties from SQL for " + target.indexName(), e);
    }
    List<ConsistencyIssue> issues = new ArrayList<>();
    for (Map.Entry<Urn, DocumentValues> entry : byUrn.entrySet()) {
      if (aspectPresent(responses.get(entry.getKey()))) {
        continue;
      }
      FieldChange change = diff(Map.of(), entry.getValue().fields(), mapped);
      if (!change.isEmpty()) {
        issueFor(entry.getKey(), entityType, Map.of(target.indexName(), change))
            .ifPresent(issues::add);
      }
    }
    return issues;
  }

  @Nonnull
  private Optional<ConsistencyIssue> issueFor(
      @Nonnull Urn urn, @Nonnull String entityType, @Nonnull Map<String, FieldChange> changes) {
    if (changes.isEmpty()) {
      return Optional.empty();
    }
    int missing = 0;
    int different = 0;
    int extra = 0;
    ObjectNode targetsNode = objectMapper.createObjectNode();
    for (Map.Entry<String, FieldChange> entry : changes.entrySet()) {
      FieldChange change = entry.getValue();
      missing += change.missing();
      different += change.different();
      extra += change.extra();
      ObjectNode indexNode = targetsNode.putObject(entry.getKey());
      ObjectNode set = indexNode.putObject("set");
      change.values().forEach(set::set);
      ArrayNode clear = indexNode.putArray("null");
      change.clear().forEach(clear::add);
    }
    return Optional.of(
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType(entityType)
            .checkId(CHECK_ID)
            .fixType(ConsistencyFixType.SYNC_STRUCTURED_PROPERTY_INDEX_FIELDS)
            .description(
                String.format(
                    "Mapped structured properties differ from SQL: %d missing, %d different, %d extra",
                    missing, different, extra))
            .metadata(Map.of(TARGETS, targetsNode.toString()))
            .build());
  }

  /**
   * {@code null} actual fields mean the document is absent. That is a mismatch only when SQL
   * projects a mapped field. Values outside {@code mapped} are ignored.
   */
  @Nonnull
  static FieldChange diff(
      @Nonnull Map<String, JsonNode> expected,
      @Nullable Map<String, JsonNode> actual,
      @Nonnull Set<String> mapped) {
    if (actual == null) {
      if (expected.isEmpty()) {
        return new FieldChange(Map.of(), List.of(), 0, 0, 0);
      }
      return new FieldChange(new LinkedHashMap<>(expected), List.of(), expected.size(), 0, 0);
    }
    Map<String, JsonNode> present = new LinkedHashMap<>();
    for (Map.Entry<String, JsonNode> entry : actual.entrySet()) {
      if (mapped.contains(entry.getKey())
          && entry.getValue() != null
          && !entry.getValue().isNull()) {
        present.put(entry.getKey(), entry.getValue());
      }
    }
    Map<String, JsonNode> values = new LinkedHashMap<>();
    List<String> clear = new ArrayList<>();
    int missing = 0;
    int different = 0;
    for (Map.Entry<String, JsonNode> entry : expected.entrySet()) {
      JsonNode found = present.get(entry.getKey());
      if (found == null) {
        values.put(entry.getKey(), entry.getValue());
        missing++;
      } else if (!sameValue(found, entry.getValue())) {
        values.put(entry.getKey(), entry.getValue());
        different++;
      }
    }
    for (String field : present.keySet()) {
      if (!expected.containsKey(field)) {
        clear.add(field);
      }
    }
    return new FieldChange(values, List.copyOf(clear), missing, different, clear.size());
  }

  /**
   * Numeric leaves compare by value. The projection writes a double, and a search {@code _source}
   * map often parses a whole number as an int. {@link JsonNode#equals} treats those as different.
   */
  static boolean sameValue(@Nullable JsonNode expected, @Nullable JsonNode actual) {
    if (expected == null || actual == null) {
      return expected == actual;
    }
    if (expected.isNumber() && actual.isNumber()) {
      return expected.decimalValue().compareTo(actual.decimalValue()) == 0;
    }
    if (expected.isArray() && actual.isArray()) {
      if (expected.size() != actual.size()) {
        return false;
      }
      for (int i = 0; i < expected.size(); i++) {
        if (!sameValue(expected.get(i), actual.get(i))) {
          return false;
        }
      }
      return true;
    }
    return expected.equals(actual);
  }

  @Nonnull
  private Optional<Map<String, JsonNode>> project(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nullable StructuredProperties aspect,
      @Nonnull IndexTarget target,
      @Nonnull Set<String> mapped) {
    if (aspect == null) {
      return Optional.of(Map.of());
    }
    AspectSpec aspectSpec = aspectSpec(opContext, urn.getEntityType());
    if (aspectSpec == null) {
      return Optional.empty();
    }
    ObjectNode document;
    try {
      document =
          transformer
              .transformAspect(opContext, urn, aspect, aspectSpec, false, projectionStamp)
              .orElseGet(objectMapper::createObjectNode);
    } catch (Exception e) {
      log.warn("Failed to project structured properties for {}: {}", urn, e.getMessage());
      return Optional.empty();
    }
    if (target.v3()) {
      try {
        StructuredPropertyUrnIdentity.project(opContext, document, aspect);
      } catch (RuntimeException e) {
        // Without these siblings, mapped __urn_name fields look extra and the fix would null them.
        log.warn("Failed to project V3 URN names for {}: {}", urn, e.getMessage());
        return Optional.empty();
      }
    }
    Map<String, JsonNode> fields = new LinkedHashMap<>();
    document
        .fields()
        .forEachRemaining(
            entry -> {
              if (mapped.contains(entry.getKey())
                  && entry.getValue() != null
                  && !entry.getValue().isNull()) {
                fields.put(entry.getKey(), entry.getValue());
              }
            });
    return Optional.of(fields);
  }

  @Nullable
  private StructuredProperties loadAspect(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    try {
      Map<Urn, EntityResponse> responses =
          entityService.getEntitiesV2(
              opContext,
              urn.getEntityType(),
              Set.of(urn),
              Set.of(STRUCTURED_PROPERTIES_ASPECT_NAME),
              false);
      return aspectOf(responses.get(urn));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to load structuredProperties for " + urn, e);
    }
  }

  @Nullable
  private static StructuredProperties aspectOf(@Nullable EntityResponse response) {
    if (!aspectPresent(response)) {
      return null;
    }
    Aspect value = response.getAspects().get(STRUCTURED_PROPERTIES_ASPECT_NAME).getValue();
    return new StructuredProperties(value.data());
  }

  public static boolean aspectPresent(@Nullable EntityResponse response) {
    if (response == null || response.getAspects() == null) {
      return false;
    }
    EnvelopedAspect aspect = response.getAspects().get(STRUCTURED_PROPERTIES_ASPECT_NAME);
    return aspect != null && aspect.getValue() != null && aspect.getValue().data() != null;
  }

  @Nullable
  private static AspectSpec aspectSpec(
      @Nonnull OperationContext opContext, @Nonnull String entityType) {
    try {
      return opContext
          .getEntityRegistry()
          .getEntitySpec(entityType)
          .getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME);
    } catch (RuntimeException e) {
      return null;
    }
  }

  @Nonnull
  List<IndexTarget> targets(@Nonnull OperationContext opContext, @Nonnull String entityType) {
    EntityIndexConfiguration entityIndex =
        elasticSearchConfiguration == null ? null : elasticSearchConfiguration.getEntityIndex();
    boolean v3 = EntitySearchIndexResolver.shouldReadV3(entityIndex);
    return List.of(
        new IndexTarget(
            EntitySearchIndexResolver.indexName(opContext, entityType, entityIndex), v3));
  }

  @Nonnull
  private String documentId(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull IndexTarget target) {
    if (target.v3()) {
      return documentIdHasher.documentId(opContext, urn);
    }
    return opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);
  }

  @Nullable
  private IndexTarget nextTarget(@Nonnull List<IndexTarget> targets, @Nonnull ScanState state) {
    for (IndexTarget target : targets) {
      if (!state.done.contains(target.indexName())) {
        return target;
      }
    }
    return null;
  }

  private static void finish(@Nonnull ScanState state, @Nonnull String indexName) {
    state.done.add(indexName);
    state.cursors.remove(indexName);
  }

  private static void advance(
      @Nonnull ScanState state,
      @Nonnull String indexName,
      @Nullable String cursor,
      @Nullable String next) {
    if (next == null || next.equals(cursor)) {
      finish(state, indexName);
    } else {
      state.cursors.put(indexName, next);
    }
  }

  @Nonnull
  private ScanState decode(@Nonnull String scrollToken) {
    try {
      if (!scrollToken.startsWith(INDEX_SCROLL_PREFIX)) {
        throw new IllegalArgumentException("missing index scroll prefix");
      }
      String payload = scrollToken.substring(INDEX_SCROLL_PREFIX.length());
      JsonNode root = objectMapper.readTree(Base64.getUrlDecoder().decode(payload));
      if (root == null || !root.isObject()) {
        throw new IllegalArgumentException("index scroll is not an object");
      }
      ScanState state = new ScanState();
      JsonNode done = root.get(DONE);
      if (done != null && done.isArray()) {
        done.forEach(node -> state.done.add(node.asText()));
      }
      JsonNode cursors = root.get(CURSOR);
      if (cursors != null && cursors.isObject()) {
        cursors
            .fields()
            .forEachRemaining(
                entry -> state.cursors.put(entry.getKey(), entry.getValue().asText()));
      }
      return state;
    } catch (Exception e) {
      throw new IllegalStateException("Unreadable structured property index scroll", e);
    }
  }

  @Nonnull
  private String encode(@Nonnull ScanState state) {
    try {
      ObjectNode root = objectMapper.createObjectNode();
      ArrayNode done = root.putArray(DONE);
      state.done.forEach(done::add);
      ObjectNode cursors = root.putObject(CURSOR);
      state.cursors.forEach(cursors::put);
      return INDEX_SCROLL_PREFIX
          + Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(objectMapper.writeValueAsBytes(root));
    } catch (Exception e) {
      throw new IllegalStateException("Unable to encode structured property index scroll", e);
    }
  }

  private void logDisabled() {
    if (disabledLogged.compareAndSet(false, true)) {
      log.info(
          "structured-property-index-mismatch skipped:"
              + " elasticsearch.index.structuredPropertyRestateEnabled is false");
    }
  }

  private static final class ScanState {
    private final Set<String> done = new LinkedHashSet<>();
    private final Map<String, String> cursors = new LinkedHashMap<>();
  }
}
