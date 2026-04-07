package com.linkedin.metadata.entity.upgrade;

import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.UNVERSIONED_ASPECT_VERSION;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Optimistic concurrency for {@link DataHubUpgradeResult} on {@link
 * Constants#DATA_HUB_UPGRADE_ENTITY_NAME} entities. Uses {@code If-Version-Match} and retries on
 * version mismatch so concurrent writers (GMS callbacks, upgrade jobs, ops API) do not silently
 * overwrite each other.
 */
@Slf4j
public final class DataHubUpgradeResultConditionalPersist {

  public static final int DEFAULT_MAX_ATTEMPTS = 10;

  @FunctionalInterface
  public interface Merge {
    /**
     * Mutates {@code resultMap} in place (a fresh mutable copy of the current aspect result, or
     * empty when creating).
     *
     * @return state to persist; null keeps the existing state from storage
     */
    @Nullable
    DataHubUpgradeState apply(
        @Nonnull Map<String, String> resultMap, @Nullable DataHubUpgradeState existingState);
  }

  private DataHubUpgradeResultConditionalPersist() {}

  /**
   * Replaces the entire {@code result} map and sets {@code explicitState} when non-null; otherwise
   * keeps the state already loaded from storage.
   */
  @Nonnull
  public static Merge replaceEntireResult(
      @Nonnull Map<String, String> newResult, @Nullable DataHubUpgradeState explicitState) {
    return (map, existingState) -> {
      map.clear();
      map.putAll(newResult);
      return explicitState != null ? explicitState : existingState;
    };
  }

  /**
   * Adds or updates one result-map entry and keeps {@code existingState} when non-null; otherwise
   * uses {@code stateFallback} (e.g. prior aspect state).
   */
  @Nonnull
  public static Merge putResultEntry(
      @Nonnull String key, @Nonnull String value, @Nullable DataHubUpgradeState stateFallback) {
    return (map, existingState) -> {
      map.put(key, value);
      return existingState != null ? existingState : stateFallback;
    };
  }

  public static void mergeAndPersist(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull Urn dataHubUpgradeUrn,
      @Nonnull Merge merge)
      throws Exception {
    mergeAndPersist(opContext, entityService, dataHubUpgradeUrn, merge, DEFAULT_MAX_ATTEMPTS);
  }

  public static void mergeAndPersist(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull Urn dataHubUpgradeUrn,
      @Nonnull Merge merge,
      int maxAttempts)
      throws Exception {
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      EnvelopedAspect env =
          entityService.getLatestEnvelopedAspect(
              opContext,
              Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
              dataHubUpgradeUrn,
              Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);

      Map<String, String> resultMap = new HashMap<>();
      DataHubUpgradeState existingState = null;
      if (env != null && env.getValue() != null) {
        DataHubUpgradeResult current = new DataHubUpgradeResult(env.getValue().data());
        if (current.hasResult() && current.getResult() != null) {
          resultMap.putAll(current.getResult());
        }
        if (current.hasState()) {
          existingState = current.getState();
        }
      }

      DataHubUpgradeState newState = merge.apply(resultMap, existingState);
      if (newState == null) {
        newState = existingState;
      }

      DataHubUpgradeResult toWrite =
          new DataHubUpgradeResult()
              .setTimestampMs(System.currentTimeMillis())
              .setState(newState, SetMode.IGNORE_NULL)
              .setResult(
                  resultMap.isEmpty() ? null : new StringMap(resultMap), SetMode.IGNORE_NULL);

      MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(dataHubUpgradeUrn);
      proposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
      proposal.setAspectName(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(toWrite));
      proposal.setChangeType(ChangeType.UPSERT);

      StringMap headers = new StringMap();
      if (env != null && env.hasSystemMetadata() && env.getSystemMetadata().getVersion() != null) {
        headers.put(HTTP_HEADER_IF_VERSION_MATCH, env.getSystemMetadata().getVersion());
      } else {
        headers.put(HTTP_HEADER_IF_VERSION_MATCH, UNVERSIONED_ASPECT_VERSION);
      }
      proposal.setHeaders(headers);

      try {
        entityService.ingestProposal(
            opContext, proposal, AuditStampUtils.createDefaultAuditStamp(), false);
        return;
      } catch (ValidationException e) {
        if (!isOptimisticLockConflict(e) || attempt == maxAttempts - 1) {
          throw e;
        }
        log.warn(
            "Retrying DataHubUpgradeResult write for {} after optimistic lock conflict (attempt {}/{})",
            dataHubUpgradeUrn,
            attempt + 1,
            maxAttempts);
        Thread.sleep(backoffMillis(attempt));
      }
    }
  }

  private static long backoffMillis(int attempt) {
    return Math.min(500L, 20L * (attempt + 1));
  }

  private static boolean isOptimisticLockConflict(ValidationException e) {
    String msg = e.getMessage();
    if (msg != null && msg.contains("Expected version")) {
      return true;
    }
    ValidationExceptionCollection coll = e.getValidationExceptionCollection();
    if (coll != null) {
      return coll.streamAllExceptions()
          .map(AspectValidationException::getMessage)
          .filter(Objects::nonNull)
          .anyMatch(m -> m.contains("Expected version"));
    }
    return false;
  }

  /** Reconstruct {@link DataHubUpgradeResult} from an enveloped aspect value, if present. */
  @Nullable
  public static DataHubUpgradeResult fromEnveloped(@Nullable EnvelopedAspect env) {
    if (env == null || env.getValue() == null) {
      return null;
    }
    return new DataHubUpgradeResult(env.getValue().data());
  }

  /** Build an {@link EnvelopedAspect} for tests or read paths that need a versioned snapshot. */
  @Nonnull
  public static EnvelopedAspect toEnveloped(
      @Nonnull DataHubUpgradeResult result, @Nonnull String systemMetadataVersion) {
    EnvelopedAspect ea = new EnvelopedAspect();
    ea.setValue(new Aspect(result.data()));
    ea.setSystemMetadata(new SystemMetadata().setVersion(systemMetadataVersion));
    return ea;
  }
}
