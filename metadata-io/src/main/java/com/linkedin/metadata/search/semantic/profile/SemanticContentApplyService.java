package com.linkedin.metadata.search.semantic.profile;

import static com.linkedin.metadata.Constants.SEMANTIC_APPLY_PROPERTY;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.common.SemanticWorkState;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.patch.builder.SemanticProfilePatchBuilder;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileEligibility.Decision;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileEligibility.Eligible;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileEligibility.Excluded;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Stores one embedding profile without rewriting its siblings. Each write carries preconditions on
 * that profile's own state, and on the entity sources it is bound to, as read before the write. A
 * write to another profile never conflicts. A conflicting change to this profile is retried once by
 * re-reading.
 */
public class SemanticContentApplyService {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Fields a newer result or removal of the same key always changes. */
  private static final List<String> RESULT_STATE_FIELDS =
      List.of("contentHash", "refreshMarker", "generatedAt");

  private static final List<String> REMOVAL_STATE_FIELDS =
      List.of("contentHash", "vectorState", "generatedAt");

  private static final List<String> WORK_STATE_FIELDS =
      List.of("modelVersion", "contentHash", "policyVersion", "workState", "generatedAt");

  private final SemanticContentStore store;
  private final SemanticProfileEligibility eligibility;
  private final SemanticProfileRegistry registry;

  public SemanticContentApplyService(
      @Nonnull SemanticContentStore store,
      @Nonnull SemanticProfileEligibility eligibility,
      @Nonnull SemanticProfileRegistry registry) {
    this.store = store;
    this.eligibility = eligibility;
    this.registry = registry;
  }

  @Nonnull
  public ApplyOutcome applyResult(
      @Nonnull OperationContext context,
      @Nonnull Urn urn,
      @Nonnull String profileKey,
      @Nonnull EmbeddingModelData profile,
      @Nonnull SemanticRefreshMarker representedMarker,
      @Nonnull String expectedPayloadHash) {
    if (!store.exists(context, urn)) {
      return ApplyOutcome.DROPPED_ENTITY_MISSING;
    }
    Set<String> watchKeys =
        SemanticProfileRefresh.watchKeys(registry.bindingFor(urn.getEntityType(), profileKey));
    return writeOnce(
        context,
        urn,
        stored -> {
          EmbeddingModelData existing = profileOf(stored, profileKey);
          if (existing == null || !expectedPayloadHash.equals(existing.getContentHash())) {
            return ApplyOutcome.DROPPED_HASH_MISMATCH;
          }
          if (existing.hasRefreshMarker()
              && markerNewer(
                  SemanticProfileRefresh.restrict(existing.getRefreshMarker(), watchKeys),
                  representedMarker)) {
            return ApplyOutcome.DROPPED_STALE_RESULT;
          }
          Decision decision = eligibility.evaluate(context, urn, profileKey, profile);
          if (decision instanceof Excluded) {
            return ApplyOutcome.DROPPED_INELIGIBLE;
          }
          EmbeddingModelData written = profile.copy();
          written.setVectorState(SemanticVectorState.READY);
          written.setWorkState(SemanticWorkState.NONE);
          written.setCurrent(
              SemanticProfileRefresh.coversBoundSources(representedMarker, stored, watchKeys));
          written.setRefreshMarker(SemanticProfileRefresh.restrict(representedMarker, watchKeys));
          written.removeDirty();
          written.setContentHash(expectedPayloadHash);
          written.setGeneratedAt(System.currentTimeMillis());
          if (decision instanceof Eligible eligible && eligible.policyVersion() != null) {
            written.setPolicyVersion(eligible.policyVersion());
          }
          SemanticProfilePatchBuilder builder = new SemanticProfilePatchBuilder();
          builder
              .expectProfileState(profileKey, existing, RESULT_STATE_FIELDS)
              .expectBoundSources(stored, watchKeys)
              .urn(urn)
              .setProfile(profileKey, toJson(written));
          return write(context, urn, builder);
        });
  }

  @Nonnull
  public ApplyOutcome applyRemoval(
      @Nonnull OperationContext context,
      @Nonnull Urn urn,
      @Nonnull String profileKey,
      @Nonnull String modelVersion,
      @Nonnull Removal removal,
      @Nullable String reason) {
    if (removal != Removal.REMOVE_KEY && (reason == null || reason.isBlank())) {
      throw new IllegalArgumentException("A profile removal requires a reason");
    }
    if (!store.exists(context, urn)) {
      return ApplyOutcome.DROPPED_ENTITY_MISSING;
    }
    return writeOnce(
        context,
        urn,
        stored -> {
          EmbeddingModelData existing = profileOf(stored, profileKey);
          SemanticProfilePatchBuilder builder = new SemanticProfilePatchBuilder();
          builder.expectProfileState(profileKey, existing, REMOVAL_STATE_FIELDS).urn(urn);
          if (removal == Removal.REMOVE_KEY) {
            if (existing == null) {
              return ApplyOutcome.ACCEPTED;
            }
            builder.removeProfile(profileKey);
          } else {
            long now = System.currentTimeMillis();
            EmbeddingModelData kept =
                existing != null
                    ? existing.copy()
                    : SemanticProfiles.baseProfile(modelVersion, now);
            kept.setVectorState(
                removal == Removal.EXCLUDE
                    ? SemanticVectorState.EXCLUDED
                    : SemanticVectorState.SKIPPED);
            kept.setChunks(new EmbeddingChunkArray());
            kept.setTotalChunks(0);
            kept.setWorkState(SemanticWorkState.NONE);
            kept.removeCurrent();
            kept.removeContentHash();
            kept.removePartial();
            kept.removeVectorDimension();
            kept.removeRefreshMarker();
            if (reason != null) {
              kept.setStateReason(reason);
            }
            kept.setGeneratedAt(now);
            builder.setProfile(profileKey, toJson(kept));
          }
          return write(context, urn, builder);
        });
  }

  @Nonnull
  public ApplyOutcome applyStateOnly(
      @Nonnull OperationContext context,
      @Nonnull Urn urn,
      @Nonnull String profileKey,
      @Nonnull String modelVersion,
      @Nonnull SemanticWorkState workState,
      @Nullable String reason,
      @Nullable String expectedPayloadHash,
      @Nullable String expectedPolicyVersion) {
    if (!store.exists(context, urn)) {
      return ApplyOutcome.DROPPED_ENTITY_MISSING;
    }
    boolean[] seen = {false};
    SemanticWorkState[] firstWork = {null};
    String[] firstReason = {null};
    Long[] firstGeneratedAt = {null};
    return writeOnce(
        context,
        urn,
        stored -> {
          EmbeddingModelData existing = profileOf(stored, profileKey);
          if (seen[0]
              && workMoved(existing, firstWork[0], reason, firstReason[0], firstGeneratedAt[0])) {
            return ApplyOutcome.DROPPED_STALE_WORK;
          }
          if (!seen[0]) {
            seen[0] = true;
            firstWork[0] = workStateOf(existing);
            firstReason[0] = reasonOf(existing);
            firstGeneratedAt[0] = generatedAtOf(existing);
          }
          SemanticProfilePatchBuilder builder = new SemanticProfilePatchBuilder();
          builder.expectProfileState(profileKey, existing, workStateFields(reason)).urn(urn);
          if (existing != null) {
            if (expectedPayloadHash != null
                && !expectedPayloadHash.equals(existing.getContentHash())) {
              return ApplyOutcome.DROPPED_STALE_WORK;
            }
            if (expectedPolicyVersion != null
                && !expectedPolicyVersion.equals(existing.getPolicyVersion())) {
              return ApplyOutcome.DROPPED_STALE_WORK;
            }
            // Field-level writes keep vectors that a concurrent result stored for the same hash.
            builder.setProfileField(profileKey, "workState", TextNode.valueOf(workState.name()));
            if (reason != null) {
              builder.setProfileField(profileKey, "stateReason", TextNode.valueOf(reason));
            }
          } else {
            EmbeddingModelData created =
                SemanticProfiles.baseProfile(modelVersion, System.currentTimeMillis());
            created.setWorkState(workState);
            if (reason != null) {
              created.setStateReason(reason);
            }
            if (expectedPayloadHash != null) {
              created.setContentHash(expectedPayloadHash);
            }
            if (expectedPolicyVersion != null) {
              created.setPolicyVersion(expectedPolicyVersion);
            }
            builder.setProfile(profileKey, toJson(created));
          }
          return write(context, urn, builder);
        });
  }

  @FunctionalInterface
  private interface Attempt {
    @Nullable
    ApplyOutcome apply(@Nullable SemanticContent content) throws Exception;
  }

  @Nonnull
  private ApplyOutcome writeOnce(
      @Nonnull OperationContext context, @Nonnull Urn urn, @Nonnull Attempt attempt) {
    try {
      ApplyOutcome outcome = attempt.apply(store.get(context, urn));
      if (outcome != null) {
        return outcome;
      }
      ApplyOutcome retried = attempt.apply(store.get(context, urn));
      return retried == null ? ApplyOutcome.DROPPED_STALE_WORK : retried;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to apply semantic content for " + urn, e);
    }
  }

  @Nullable
  private ApplyOutcome write(
      @Nonnull OperationContext context,
      @Nonnull Urn urn,
      @Nonnull SemanticProfilePatchBuilder builder) {
    MetadataChangeProposal patch = builder.build();
    stampApply(patch);
    return store.patch(context, urn, patch) ? ApplyOutcome.ACCEPTED : null;
  }

  @Nullable
  private static EmbeddingModelData profileOf(
      @Nullable SemanticContent content, @Nonnull String profileKey) {
    if (content == null || !content.hasEmbeddings()) {
      return null;
    }
    return content.getEmbeddings().get(profileKey);
  }

  @Nonnull
  private static JsonNode toJson(@Nonnull EmbeddingModelData profile) throws Exception {
    return MAPPER.readTree(RecordUtils.toJsonString(profile));
  }

  static boolean markerNewer(
      @Nullable SemanticRefreshMarker stored, @Nonnull SemanticRefreshMarker represented) {
    if (stored == null) {
      return false;
    }
    for (String source : stored.getSources().keySet()) {
      long storedAt = stored.getSources().get(source);
      Long representedAt = represented.getSources().get(source);
      if (representedAt == null || storedAt > representedAt) {
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private static List<String> workStateFields(@Nullable String reason) {
    if (reason == null) {
      return WORK_STATE_FIELDS;
    }
    List<String> fields = new ArrayList<>(WORK_STATE_FIELDS);
    fields.add("stateReason");
    return fields;
  }

  /**
   * A retry must not write the caller's state over a result that landed after the read. {@code
   * generatedAt} changes on every result, including one that leaves {@code workState} at {@code
   * NONE}.
   */
  private static boolean workMoved(
      @Nullable EmbeddingModelData existing,
      @Nullable SemanticWorkState firstWork,
      @Nullable String reason,
      @Nullable String firstReason,
      @Nullable Long firstGeneratedAt) {
    if (!Objects.equals(firstWork, workStateOf(existing))) {
      return true;
    }
    if (!Objects.equals(firstGeneratedAt, generatedAtOf(existing))) {
      return true;
    }
    return reason != null && !Objects.equals(firstReason, reasonOf(existing));
  }

  @Nullable
  private static SemanticWorkState workStateOf(@Nullable EmbeddingModelData existing) {
    return existing == null || !existing.hasWorkState() ? null : existing.getWorkState();
  }

  @Nullable
  private static String reasonOf(@Nullable EmbeddingModelData existing) {
    return existing == null || !existing.hasStateReason() ? null : existing.getStateReason();
  }

  @Nullable
  private static Long generatedAtOf(@Nullable EmbeddingModelData existing) {
    return existing == null || !existing.hasGeneratedAt() ? null : existing.getGeneratedAt();
  }

  private static void stampApply(@Nonnull MetadataChangeProposal proposal) {
    SystemMetadata metadata =
        proposal.hasSystemMetadata()
            ? proposal.getSystemMetadata()
            : SystemMetadataUtils.createDefaultSystemMetadata();
    if (!metadata.hasProperties()) {
      metadata.setProperties(new StringMap());
    }
    metadata.getProperties().put(SEMANTIC_APPLY_PROPERTY, "true");
    proposal.setSystemMetadata(metadata);
  }

  /** How a profile leaves the searchable set. {@link #REMOVE_KEY} drops the key entirely. */
  public enum Removal {
    EXCLUDE,
    SKIP,
    REMOVE_KEY
  }
}
