package com.linkedin.gms.factory.search;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityTypeDocumentCount;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexMigrationStamp;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SearchV3CutoverStamp;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SearchV3ReadHoldPolicy;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SearchV3ReadHoldPolicy.Decision;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

/**
 * Keeps a process on V2 reads while a V2 entity index has no complete V3 counterpart.
 *
 * <p>Runs once per process, before any consumer reads the search flags. When V3 reads are
 * configured and some existing V2 index is paired with a V3 index that is not stamped complete, it
 * sets V2 enabled, turns keyword V3 reads off, and turns hybrid reads off, on the live
 * configuration. A process that finds no V2 entity index is left alone, so a fresh install reads
 * V3. When every paired V3 index is complete, each V2 index is stamped retired and this process
 * does not hold; a later start that sees those stamps does not hold either, even if a V3 mark is
 * gone. A V2 cluster that cannot be listed fails startup. Until the V2 indices are retired, a
 * paired V3 index that cannot be read keeps its pair held. Once they are retired, a V3 index that
 * cannot be read stops a process that serves search rather than holding. A process that builds
 * indices and serves no reads continues, so it can recreate the index. An index marked incomplete
 * counts as unreadable, with or without documents and when its count cannot be read, so readers
 * stay down until catch-up marks it complete. An index with no migration mark and a positive count
 * stays readable. An install that already reads V3 can acknowledge the cutover. Once every V2 index
 * is stamped retired, later starts trust that stamp and do not consult the acknowledgement again,
 * even when the flag stays set. Before that, the V2 indices are stamped retired and reads stay on
 * V3 when every paired V3 index exists, is not marked incomplete, and at least one holds a document
 * of its entity type. A count below the V2 total still qualifies. A type whose V2 index also has no
 * documents is unused, so it does not have to. A type whose V2 index has documents and whose V3
 * index has none keeps the hold when no other type is ready, and stops readers when another type
 * is. A V3 index that cannot be read stops a process that serves search instead of holding. A
 * failed write of the retired stamp fails startup, so this process does not stop V2 writes without
 * leaving the record.
 */
@Slf4j
public class SearchV3ReadHold {

  /** Highest-precedence source system info reads, so a hold is visible as the effective flags. */
  public static final String PROPERTY_SOURCE = "searchV3ReadHold";

  public static final String V2_ENABLED = "elasticsearch.entityIndex.v2.enabled";
  static final String V3_KEYWORD_READ = "elasticsearch.entityIndex.v3.keywordReadEnabled";
  static final String V3_HYBRID_READ = "elasticsearch.entityIndex.v3.hybridReadEnabled";

  /** Env var operators set on an install that already reads V3 with V2 off. */
  public static final String CUTOVER_ACKNOWLEDGED_ENV =
      "ELASTICSEARCH_ENTITY_INDEX_V3_CUTOVER_ACKNOWLEDGED";

  private final SearchClusterRegistry searchClusterRegistry;
  private final IndexConvention indexConvention;
  private final EntityRegistry entityRegistry;
  @Nullable private final ConfigurableEnvironment environment;

  /**
   * True for system update, restore, and LoadIndices. Those processes serve no reads, so a missing
   * V3 index after the cutover does not stop them.
   */
  private final boolean rebuildMissingIndices;

  public SearchV3ReadHold(
      @Nonnull SearchClusterRegistry searchClusterRegistry,
      @Nonnull IndexConvention indexConvention,
      @Nonnull EntityRegistry entityRegistry,
      @Nullable ConfigurableEnvironment environment,
      boolean rebuildMissingIndices) {
    this.searchClusterRegistry = searchClusterRegistry;
    this.indexConvention = indexConvention;
    this.entityRegistry = entityRegistry;
    this.environment = environment;
    this.rebuildMissingIndices = rebuildMissingIndices;
  }

  public void applyTo(@Nonnull ElasticSearchConfiguration elasticSearchConfiguration) {
    EntityIndexConfiguration entityIndex = elasticSearchConfiguration.getEntityIndex();
    if (entityIndex == null || !EntitySearchIndexResolver.shouldReadV3(entityIndex)) {
      return;
    }
    Map<String, PairedIndex> v2ToV3 = pairedIndices();
    if (v2ToV3.isEmpty()) {
      return;
    }
    Set<String> v2Indices = v2ToV3.keySet();
    Set<String> retired = retiredV2Indices(v2Indices);
    Set<String> pairedV3Indices = new TreeSet<>();
    for (PairedIndex paired : v2ToV3.values()) {
      pairedV3Indices.add(paired.v3Index());
    }
    MigrationRead migration = migrationStates(pairedV3Indices);
    // A retired stamp is the record later starts trust. The flag is a one-start statement, so a
    // chart that leaves it set must not compare document counts again.
    if (cutoverAcknowledged(entityIndex) && !retired.containsAll(v2Indices)) {
      AcknowledgementResult acknowledgement = classifyAcknowledgement(v2ToV3, migration);
      switch (acknowledgement.outcome) {
        case APPLY -> {
          retire(v2Indices);
          log.info(
              "Search V3 cutover acknowledged. V2 indices {} marked retired; this process keeps"
                  + " reading V3",
              v2Indices);
          return;
        }
        case UNREAD, REFUSE -> {
          failOrRebuild(acknowledgement.refusalMessage);
          return;
        }
        case FIRST_MIGRATION -> {}
      }
    }
    Set<String> notReadable = migration.notReadable();
    Decision decision =
        SearchV3ReadHoldPolicy.decide(
            v2Indices, retired, pairedV3Indices, migration.states, notReadable);
    switch (decision) {
      case LEAVE -> {}
      case HOLD -> {
        hold(
            entityIndex,
            "paired V3 indices not stamped complete "
                + SearchV3ReadHoldPolicy.incompleteV3Indices(pairedV3Indices, migration.states));
        warnIfUnmarkedV3HasDocuments(pairedV3Indices, migration);
      }
      case COMMIT -> {
        retire(v2Indices);
        log.info(
            "Search V3 read hold not needed: every paired V3 index is stamped complete. V2 indices"
                + " {} marked retired",
            v2Indices);
      }
      case CUT_OVER ->
          log.info("Search V3 read hold not needed: V2 indices {} are marked retired", v2Indices);
      case FAIL ->
          failOrRebuild(
              "Search V3 cutover is recorded on "
                  + retired
                  + ", but paired V3 indices "
                  + notReadable
                  + " cannot be read.");
      default -> throw new IllegalStateException("Unhandled read-hold decision " + decision);
    }
  }

  private static boolean cutoverAcknowledged(EntityIndexConfiguration entityIndex) {
    return entityIndex.getV3() != null && entityIndex.getV3().isCutoverAcknowledged();
  }

  /**
   * The acknowledgement records a cutover that already happened. It is not consulted once every V2
   * index is stamped retired: later starts trust that stamp, even when the flag stays set. Before
   * that, it applies when every paired V3 index exists, is not marked incomplete, and at least one
   * holds a document of its entity type. The count is that entity type, in whichever index the pair
   * names, and it is not compared with the V2 total: hard deletes land only in V3, so a smaller V3
   * count can still be caught up. A type whose V2 index also has no documents is unused.
   * BuildIndices creates a V2 index for every entity type, including ones the install never wrote,
   * so that empty index is not a type that must have V3 documents. A type whose V2 index has
   * documents and whose V3 index has none is not ready. When no pair is ready, this stays a first
   * migration and the hold stays. When some pair is ready and another is missing, incomplete, empty
   * of a type its V2 index still holds, or cannot be counted, readers stop instead of serving V2. A
   * V3 index whose listing or mapping cannot be read stops readers too, even when every other pair
   * is ready: startup cannot tell whether it exists.
   */
  private AcknowledgementResult classifyAcknowledgement(
      Map<String, PairedIndex> v2ToV3, MigrationRead migration) {
    if (!migration.readFailed.isEmpty()) {
      String message =
          "Search V3 cutover acknowledgement cannot be applied. Paired V3 indices "
              + migration.readFailed
              + " cannot be read, so startup cannot tell whether they exist.";
      log.warn(message);
      return new AcknowledgementResult(Acknowledgement.UNREAD, message);
    }
    SearchClientShim<?> v2Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2);
    SearchClientShim<?> v3Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V3);
    boolean anyReady = false;
    boolean anyUnknown = false;
    List<String> unknown = new ArrayList<>();
    List<String> empty = new ArrayList<>();
    for (Map.Entry<String, PairedIndex> pair : v2ToV3.entrySet()) {
      String v2Index = pair.getKey();
      String v3Index = pair.getValue().v3Index();
      String entityType = pair.getValue().entityType();
      if (migration.unreadable.contains(v3Index)) {
        continue;
      }
      Long v3Count = EntityTypeDocumentCount.count(v3Client, v3Index, entityType);
      if (v3Count == null) {
        anyUnknown = true;
        unknown.add(v3Index);
        continue;
      }
      if (v3Count < 1) {
        // A V2 index holds one entity type and no _entityType field: its total is the type's count.
        Long v2Count = documentCount(v2Client, v2Index);
        if (v2Count == null) {
          anyUnknown = true;
          unknown.add(v2Index);
          continue;
        }
        // An empty V2 index was created for a type the install never wrote.
        if (v2Count < 1) {
          continue;
        }
        empty.add(v3Index);
        continue;
      }
      anyReady = true;
    }
    boolean anyNotReady = !migration.unreadable.isEmpty() || !empty.isEmpty();
    if (anyReady && !anyNotReady && !anyUnknown) {
      return new AcknowledgementResult(Acknowledgement.APPLY, null);
    }
    if (anyReady || anyUnknown) {
      String message = acknowledgementRefusal(migration.unreadable, empty, unknown, anyReady);
      log.warn(message);
      return new AcknowledgementResult(Acknowledgement.REFUSE, message);
    }
    if (!migration.unreadable.isEmpty()) {
      log.warn(
          "Search V3 cutover acknowledgement does not apply: paired V3 indices {} are missing or"
              + " incomplete, so this stays a first migration and startup keeps reading V2",
          migration.unreadable);
    }
    if (!empty.isEmpty()) {
      log.warn(
          "Search V3 cutover acknowledgement does not apply: paired V3 indices {} have no"
              + " documents of their entity type. Startup keeps reading V2",
          empty);
    }
    return new AcknowledgementResult(Acknowledgement.FIRST_MIGRATION, null);
  }

  private static String acknowledgementRefusal(
      Set<String> unreadable, List<String> empty, List<String> unknown, boolean anyReady) {
    StringBuilder message =
        new StringBuilder("Search V3 cutover acknowledgement cannot be applied.");
    if (!unreadable.isEmpty()) {
      message
          .append(" Paired V3 indices ")
          .append(unreadable)
          .append(" are missing or incomplete.");
    }
    if (!empty.isEmpty()) {
      message
          .append(" Paired V3 indices ")
          .append(empty)
          .append(" have no documents of their entity type.");
    }
    if (!unknown.isEmpty()) {
      message.append(" Cannot count documents in ").append(unknown).append(".");
    }
    if (anyReady) {
      message.append(
          " Another paired V3 index has documents of its entity type, so reads stay off V2.");
    }
    return message.toString();
  }

  private void failOrRebuild(String message) {
    if (rebuildMissingIndices) {
      log.warn(
          "{} This process builds indices and serves no reads, so startup continues and"
              + " BuildIndices can recreate them.",
          message);
      return;
    }
    throw new IllegalStateException(message + " Startup stops rather than move reads back to V2.");
  }

  private enum Acknowledgement {
    APPLY,
    FIRST_MIGRATION,
    REFUSE,
    UNREAD
  }

  /** The V3 index a V2 index is paired with, and the entity type whose documents it compares. */
  private record PairedIndex(String v3Index, String entityType) {}

  private static final class AcknowledgementResult {
    private final Acknowledgement outcome;
    @Nullable private final String refusalMessage;

    private AcknowledgementResult(Acknowledgement outcome, @Nullable String refusalMessage) {
      this.outcome = outcome;
      this.refusalMessage = refusalMessage;
    }
  }

  /**
   * A V3 index with documents and no migration mark is the shape of an install that already reads
   * V3. The hold still applies: only the acknowledgement, or a retired stamp, keeps reads there.
   */
  private static void warnIfUnmarkedV3HasDocuments(
      Set<String> pairedV3Indices, MigrationRead migration) {
    Set<String> unmarkedWithDocuments = new TreeSet<>();
    for (String v3Index : pairedV3Indices) {
      if (migration.unreadable.contains(v3Index) || migration.states.containsKey(v3Index)) {
        continue;
      }
      Long count = migration.documentCounts.get(v3Index);
      if (count != null && count > 0) {
        unmarkedWithDocuments.add(v3Index);
      }
    }
    if (unmarkedWithDocuments.isEmpty()) {
      return;
    }
    log.warn(
        "Search V3 indices {} already hold documents and have no migration mark. If this install"
            + " was already reading V3 with V2 off, set {} so startup does not serve the stale V2"
            + " copy. Otherwise this process keeps reading V2.",
        unmarkedWithDocuments,
        CUTOVER_ACKNOWLEDGED_ENV);
  }

  /**
   * Existing V2 entity indices and the V3 index each is paired with. A V2 cluster that cannot be
   * listed fails startup rather than reading as a fresh install.
   */
  private Map<String, PairedIndex> pairedIndices() {
    SearchClientShim<?> v2Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2);
    Map<String, PairedIndex> paired = new TreeMap<>();
    for (EntitySpec spec : entityRegistry.getEntitySpecs().values()) {
      String v2Index =
          indexConvention.getEntityIndexName(OperationFingerprint.EMPTY, spec.getName());
      try {
        if (indexExists(v2Client, v2Index)) {
          paired.put(
              v2Index,
              new PairedIndex(
                  indexConvention.getEntityIndexNameV3(
                      OperationFingerprint.EMPTY, V3IndexKeys.resolve(spec)),
                  spec.getName()));
        }
      } catch (IOException e) {
        throw new IllegalStateException(
            "Cannot list V2 entity index "
                + v2Index
                + "; startup stops rather than assume a"
                + " fresh install",
            e);
      }
    }
    return paired;
  }

  /**
   * V2 indices whose cutover stamp is retired. A mapping that cannot be read fails startup: holding
   * or not both assume an answer the cluster did not give.
   */
  private Set<String> retiredV2Indices(Set<String> v2Indices) {
    SearchClientShim<?> v2Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2);
    Set<String> retired = new TreeSet<>();
    for (String v2Index : v2Indices) {
      try {
        if (SearchV3CutoverStamp.retired(mappingSource(v2Client, v2Index))) {
          retired.add(v2Index);
        }
      } catch (IOException | RuntimeException e) {
        throw new IllegalStateException(
            "Cannot read the cutover stamp of V2 index "
                + v2Index
                + "; startup stops rather than guess whether reads already moved to V3",
            e);
      }
    }
    return retired;
  }

  /**
   * Writes the retired stamp onto each V2 index that does not have it. A failed write fails
   * startup, before this process stops writing V2.
   */
  private void retire(Set<String> v2Indices) {
    SearchClientShim<?> v2Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2);
    for (String v2Index : v2Indices) {
      try {
        Map<String, Object> current = mappingSource(v2Client, v2Index);
        if (SearchV3CutoverStamp.retired(current)) {
          continue;
        }
        v2Client.putIndexMapping(
            OperationFingerprint.EMPTY,
            new PutMappingRequest(v2Index)
                .source(Map.of(V3DocumentIdScheme.META, SearchV3CutoverStamp.retiredMeta(current))),
            RequestOptions.DEFAULT);
        log.info("Index {}: V2 index marked retired", v2Index);
      } catch (IOException | RuntimeException e) {
        throw new IllegalStateException(
            "Cannot record the Search V3 cutover on V2 index "
                + v2Index
                + "; startup stops rather than stop V2 writes without that record",
            e);
      }
    }
  }

  /**
   * Migration state per paired V3 index. An index that is missing, or marked incomplete, is
   * recorded as unreadable. Incomplete stays unreadable when the index has documents and when its
   * count cannot be read, so a repair is not served until catch-up marks it complete. An index
   * whose listing or mapping read throws is recorded as read failed: startup does not know whether
   * it exists. Either way a cutover that is already on record can fail startup instead of serving
   * an index that is not ready.
   */
  private MigrationRead migrationStates(Set<String> v3Indices) {
    SearchClientShim<?> v3Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V3);
    MigrationRead read = new MigrationRead();
    for (String v3Index : v3Indices) {
      try {
        if (!indexExists(v3Client, v3Index)) {
          log.warn("Paired V3 index {} does not exist", v3Index);
          read.unreadable.add(v3Index);
          continue;
        }
        IndexMigrationStamp.read(mappingSource(v3Client, v3Index))
            .ifPresent(state -> read.states.put(v3Index, state));
        Long count = documentCount(v3Client, v3Index);
        if (count != null) {
          read.documentCounts.put(v3Index, count);
        }
        if (IndexMigrationStamp.INCOMPLETE.equals(read.states.get(v3Index))) {
          log.warn(
              "Paired V3 index {} is marked incomplete, so readers stay down until it is marked"
                  + " complete",
              v3Index);
          read.unreadable.add(v3Index);
        }
      } catch (IOException | RuntimeException e) {
        log.warn("Cannot read the migration stamp of V3 index {}", v3Index, e);
        read.readFailed.add(v3Index);
      }
    }
    return read;
  }

  /**
   * Document count, or null when the count cannot be read. A missing count does not by itself make
   * the index unreadable: the caller decides whether it can proceed without one.
   */
  @Nullable
  private static Long documentCount(SearchClientShim<?> client, String indexName) {
    try {
      CountResponse response =
          client.count(
              OperationFingerprint.EMPTY, new CountRequest(indexName), RequestOptions.DEFAULT);
      return response.getCount();
    } catch (IOException | RuntimeException e) {
      log.warn("Cannot count documents in index {}", indexName, e);
      return null;
    }
  }

  private static final class MigrationRead {
    private final Map<String, String> states = new HashMap<>();
    private final Set<String> unreadable = new TreeSet<>();
    private final Set<String> readFailed = new TreeSet<>();
    private final Map<String, Long> documentCounts = new HashMap<>();

    private Set<String> notReadable() {
      Set<String> all = new TreeSet<>(unreadable);
      all.addAll(readFailed);
      return all;
    }
  }

  private static boolean indexExists(SearchClientShim<?> client, String indexName)
      throws IOException {
    return client.indexExists(
        OperationFingerprint.EMPTY, new GetIndexRequest(indexName), RequestOptions.DEFAULT);
  }

  private static Map<String, Object> mappingSource(SearchClientShim<?> client, String indexName)
      throws IOException {
    return client
        .getIndexMapping(
            OperationFingerprint.EMPTY,
            new GetMappingsRequest().indices(indexName),
            RequestOptions.DEFAULT)
        .mappings()
        .values()
        .stream()
        .map(MappingMetadata::getSourceAsMap)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(Map.of());
  }

  private void hold(EntityIndexConfiguration entityIndex, String reason) {
    for (EntityIndexConfiguration target : entityIndexesToHold(entityIndex)) {
      holdEntityIndex(target);
    }
    publish(environment);
    log.warn(
        "Search V3 read hold: {}. This process reads and writes V2 and keeps V3 reads off. Reads"
            + " move to V3 on the next start once every paired V3 index is stamped complete.",
        reason);
  }

  /**
   * The shared entity index plus each cluster's effective copy. A cluster with its own analyzer or
   * mapping file gets a copy when the registry is built, before this hold runs, so the copy must
   * carry the same flags or that cluster's V2 builders keep skipping V2 writes and mappings.
   */
  private Set<EntityIndexConfiguration> entityIndexesToHold(EntityIndexConfiguration shared) {
    Set<EntityIndexConfiguration> targets = Collections.newSetFromMap(new IdentityHashMap<>());
    targets.add(shared);
    for (SearchClusterRegistry.ClusterConnection connection :
        searchClusterRegistry.allConnections()) {
      EntityIndexConfiguration copy = connection.getConfig().getEntityIndex();
      if (copy != null) {
        targets.add(copy);
      }
    }
    return targets;
  }

  private static void holdEntityIndex(EntityIndexConfiguration entityIndex) {
    if (entityIndex.getV2() == null) {
      entityIndex.setV2(new EntityIndexVersionConfiguration());
    }
    if (entityIndex.getV3() == null) {
      entityIndex.setV3(new EntityIndexVersionConfiguration());
    }
    entityIndex.getV2().setEnabled(true);
    entityIndex.getV3().setKeywordReadEnabled(false);
    entityIndex.getV3().setHybridReadEnabled(false);
  }

  /**
   * The configured value of {@code name}, ignoring the hold overlay. {@code @Value} and {@code
   * Environment.getProperty} see the overlay once {@link #publish} has run.
   */
  public static boolean configuredBoolean(
      @Nullable ConfigurableEnvironment environment, @Nonnull String name, boolean defaultValue) {
    if (environment == null) {
      return defaultValue;
    }
    MutablePropertySources sources = environment.getPropertySources();
    PropertySource<?> hold = sources.remove(PROPERTY_SOURCE);
    try {
      return environment.getProperty(name, Boolean.class, defaultValue);
    } finally {
      if (hold != null) {
        sources.addFirst(hold);
      }
    }
  }

  /** Conditions are already evaluated. This source is for system info and later property reads. */
  private static void publish(@Nullable ConfigurableEnvironment environment) {
    if (environment == null) {
      return;
    }
    Map<String, Object> flags = new HashMap<>();
    flags.put(V2_ENABLED, "true");
    flags.put(V3_KEYWORD_READ, "false");
    flags.put(V3_HYBRID_READ, "false");
    if (environment.getPropertySources().contains(PROPERTY_SOURCE)) {
      environment.getPropertySources().remove(PROPERTY_SOURCE);
    }
    environment.getPropertySources().addFirst(new MapPropertySource(PROPERTY_SOURCE, flags));
  }
}
