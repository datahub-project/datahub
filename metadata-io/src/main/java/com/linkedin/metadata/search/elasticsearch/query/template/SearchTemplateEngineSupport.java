package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Whether the Search V3 engine can run a vector template's kNN request on the DataHub index:
 * Elasticsearch, or OpenSearch 3.5 or later. Before 3.5 the k-NN plugin applies a nested kNN
 * pre-filter in the chunk scope when it names an underscore-prefixed field such as {@code
 * _entityType}, which a vector rung's filter always does, and there the filter can leave out
 * documents it should match, so a page can come back empty or incomplete, depending on the filter.
 *
 * <p>The engine is read on the first {@link #read}, and kept once a read decides it: Elasticsearch,
 * or an OpenSearch version that parses. An engine type or version that cannot be read or parsed, as
 * restricted cluster info leaves the version, is {@link Support#UNKNOWN}, and the engine is read
 * again after {@link #ENGINE_READ_RETRY}. So is a version of 7 or above: OpenSearch never shipped
 * one, and a cluster in compatibility mode reports 7.10.2 whatever it runs. Each owner holds one
 * instance, which logs the owner's warnings once for each reason.
 */
@Slf4j
public final class SearchTemplateEngineSupport {

  /** What a read of the engine decided. */
  public enum Support {
    /** Elasticsearch, or OpenSearch 3.5 or later. */
    SUPPORTED,
    /** An OpenSearch version before 3.5. */
    UNSUPPORTED,
    /** The engine type or the OpenSearch version could not be read. */
    UNKNOWN
  }

  // An unreadable engine version is read again after this, as the OpenSearch shim retries one for
  // nested expansion: each failed read is a cluster request and a logged warning
  private static final Duration ENGINE_READ_RETRY = Duration.ofMinutes(1);

  private final String unsupportedWarning;
  private final String unknownWarning;
  private final LongSupplier currentTimeMillis;
  // Kept once a read decides it
  @Nullable private volatile Support decided;
  // Written under this; read without it so an undecided engine with no read due takes no lock
  private volatile long nextReadMillis;
  // Guarded by this
  private final Set<String> warnedReasons = new HashSet<>();

  /**
   * @param unsupportedWarning logged once, with the OpenSearch version, when it is before 3.5
   * @param unknownWarning logged once for each engine and version that cannot be read, with them
   * @param currentTimeMillis the clock the retry reads
   */
  public SearchTemplateEngineSupport(
      @Nonnull String unsupportedWarning,
      @Nonnull String unknownWarning,
      @Nonnull LongSupplier currentTimeMillis) {
    this.unsupportedWarning = unsupportedWarning;
    this.unknownWarning = unknownWarning;
    this.currentTimeMillis = currentTimeMillis;
  }

  /** The engine as the last read decided it, read now when no read has and the retry is due. */
  @Nonnull
  public Support read(@Nonnull OperationContext opContext) {
    final Support support = decided;
    if (support != null) {
      return support;
    }
    // Undecided with no read due: every vector rung asks, so answer without the lock
    return currentTimeMillis.getAsLong() < nextReadMillis ? Support.UNKNOWN : readEngine(opContext);
  }

  @Nonnull
  private synchronized Support readEngine(@Nonnull OperationContext opContext) {
    if (decided != null) {
      return decided;
    }
    final long now = currentTimeMillis.getAsLong();
    if (now < nextReadMillis) {
      return Support.UNKNOWN;
    }
    final SearchClientShim<?> client =
        SearchClients.forComponent(opContext, SearchComponent.SEARCH_V3);
    final SearchClientShim.SearchEngineType engine = client.getEngineType();
    if (engine != null && engine.isElasticsearch()) {
      decided = Support.SUPPORTED;
      return decided;
    }
    // The version is read on the request thread while holding this lock, so a cluster info call
    // that hangs holds up the requests that arrive while that read is due, for up to the client's
    // socket timeout, at most once a minute. Between reads, an undecided engine answers without the
    // lock.
    // On a cluster that blocks the version read, the shim logs the failure with a stack trace each
    // time it is asked, so this retry repeats those log entries on each GMS instance about once a
    // minute until a read succeeds.
    final String version = engine != null && engine.isOpenSearch() ? engineVersion(client) : null;
    if (OpenSearchSearchClientShim.supportsUnderscoreFieldsInNestedKnnFilter(version)) {
      decided = Support.SUPPORTED;
      return decided;
    }
    if (before35(version)) {
      log.warn(unsupportedWarning, version);
      decided = Support.UNSUPPORTED;
      return decided;
    }
    nextReadMillis = now + ENGINE_READ_RETRY.toMillis();
    final String reason =
        engine + " version " + (version == null || version.isBlank() ? "unknown" : version);
    if (warnedReasons.add(reason)) {
      log.warn(unknownWarning, reason);
    }
    return Support.UNKNOWN;
  }

  /**
   * An OpenSearch version that parses and comes before 3.5, a 3.5.0 prerelease included. A version
   * of 7 or above is not: compatibility mode reports 7.10.2 whatever the cluster runs.
   */
  private static boolean before35(@Nullable String version) {
    return OpenSearchSearchClientShim.isParseableVersion(version)
        && !OpenSearchSearchClientShim.supportsUnderscoreFieldsInNestedKnnFilter(version)
        && Integer.parseInt(version.substring(0, version.indexOf('.'))) < 7;
  }

  @Nullable
  private static String engineVersion(@Nonnull SearchClientShim<?> client) {
    try {
      return client.getEngineVersion();
    } catch (IOException e) {
      return null;
    }
  }
}
