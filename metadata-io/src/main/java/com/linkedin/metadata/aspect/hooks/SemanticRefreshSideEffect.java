package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.builder.SemanticContentPatchBuilder;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.search.semantic.profile.ContentBinding;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRefresh;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * After a source aspect commits, moves the entity refresh marker forward for each watched path the
 * write changed. Only paths watched by a stored, non-external profile count, so an entity with no
 * {@code semanticContent} gets no marker; backfill covers a profile created later.
 *
 * <p>The marker moves through a {@code semanticContent} patch sent with {@code If-Match: *}, so it
 * never creates the aspect or the entity. It goes through the normal proposal produce path: the
 * source write's response waits until the proposal is queued, and a failed send fails that
 * response. {@link SemanticRefreshMarkerHook} then decides {@code current} on the marker write.
 */
@Setter
@Getter
@Accessors(chain = true)
public class SemanticRefreshSideEffect extends MCPSideEffect {

  @Nonnull private AspectPluginConfig config;
  @Nonnull private SemanticProfileRegistry registry;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    Map<Urn, List<MCLItem>> sourcesByUrn = new LinkedHashMap<>();
    for (MCLItem item : mclItems) {
      if (SEMANTIC_CONTENT_ASPECT_NAME.equals(item.getAspectName())
          || item.getChangeType() == ChangeType.RESTATE) {
        continue;
      }
      sourcesByUrn.computeIfAbsent(item.getUrn(), ignored -> new ArrayList<>()).add(item);
    }
    List<MCPItem> patches = new ArrayList<>();
    sourcesByUrn.forEach(
        (urn, sources) -> {
          MCPItem patch = markerPatch(operationContext, urn, sources, retrieverContext);
          if (patch != null) {
            patches.add(patch);
          }
        });
    return patches.stream();
  }

  @Nullable
  private MCPItem markerPatch(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> sources,
      @Nonnull RetrieverContext retrieverContext) {
    Aspect stored =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(operationContext, urn, SEMANTIC_CONTENT_ASPECT_NAME);
    if (stored == null) {
      return null;
    }
    SemanticContent content = new SemanticContent(stored.data());
    if (!content.hasEmbeddings() || content.getEmbeddings().isEmpty()) {
      return null;
    }
    List<ContentBinding> bindings = watchedBindings(urn.getEntityType(), content);
    if (bindings.isEmpty()) {
      return null;
    }
    Map<String, SemanticSourceToken> changed = new LinkedHashMap<>();
    AuditStamp latest = null;
    for (MCLItem source : sources) {
      AuditStamp auditStamp = source.getAuditStamp();
      if (auditStamp == null) {
        continue;
      }
      Set<String> watchKeys = changedWatchKeys(bindings, source);
      if (watchKeys.isEmpty()) {
        continue;
      }
      SemanticSourceToken token = sourceToken(source, auditStamp);
      watchKeys.forEach(key -> changed.merge(key, token, SemanticProfileRefresh::later));
      if (latest == null || auditStamp.getTime() > latest.getTime()) {
        latest = auditStamp;
      }
    }
    if (changed.isEmpty()) {
      return null;
    }
    SemanticContentPatchBuilder builder = new SemanticContentPatchBuilder().urn(urn);
    changed.forEach(builder::setRefreshSource);
    return PatchItemImpl.builder()
        .build(
            builder.ifAspectExists().build(),
            latest,
            retrieverContext.getAspectRetriever().getEntityRegistry());
  }

  @Nonnull
  private List<ContentBinding> watchedBindings(
      @Nonnull String entityType, @Nonnull SemanticContent content) {
    List<ContentBinding> bindings = new ArrayList<>();
    for (String key : content.getEmbeddings().keySet()) {
      ContentBinding binding = registry.bindingFor(entityType, key);
      if (!binding.isExternal()) {
        bindings.add(binding);
      }
    }
    return bindings;
  }

  @Nonnull
  private static Set<String> changedWatchKeys(
      @Nonnull List<ContentBinding> bindings, @Nonnull MCLItem source) {
    DataMap before = data(source.getPreviousRecordTemplate());
    DataMap after =
        source.getChangeType() == ChangeType.DELETE ? null : data(source.getRecordTemplate());
    Set<String> keys = new HashSet<>();
    for (ContentBinding binding : bindings) {
      keys.addAll(
          SemanticProfileRefresh.changedWatchKeys(binding, source.getAspectName(), before, after));
    }
    return keys;
  }

  /**
   * The source aspect version this change produced. A delete records the deleted version plus one
   * under the deleted aspect's creation time, so it orders after everything it removed and before a
   * recreate, whose creation time is later. A stored row with no {@code aspectCreated} carries its
   * {@code createdOn} on the previous metadata. The audit time is used only when neither side has a
   * creation time.
   */
  @Nonnull
  static SemanticSourceToken sourceToken(@Nonnull MCLItem source, @Nonnull AuditStamp auditStamp) {
    boolean delete = source.getChangeType() == ChangeType.DELETE;
    SystemMetadata current = delete ? null : source.getSystemMetadata();
    SystemMetadata previous = source.getPreviousSystemMetadata();
    Long createdTime = createdTime(current);
    if (createdTime == null) {
      createdTime = createdTime(previous);
    }
    if (createdTime == null) {
      createdTime = auditStamp.getTime();
    }
    Long version = version(current);
    if (version == null) {
      Long previousVersion = version(previous);
      version = previousVersion == null ? 1 : previousVersion + 1;
    }
    return new SemanticSourceToken().setCreatedTime(createdTime).setVersion(version);
  }

  @Nullable
  private static Long createdTime(@Nullable SystemMetadata systemMetadata) {
    if (systemMetadata == null) {
      return null;
    }
    AuditStamp created = systemMetadata.getAspectCreated(GetMode.NULL);
    return created == null ? null : created.getTime();
  }

  @Nullable
  private static Long version(@Nullable SystemMetadata systemMetadata) {
    String version = systemMetadata == null ? null : systemMetadata.getVersion(GetMode.NULL);
    if (version == null) {
      return null;
    }
    try {
      return Long.parseLong(version);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Nullable
  private static DataMap data(@Nullable RecordTemplate record) {
    return record == null ? null : record.data();
  }
}
