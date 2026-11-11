package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
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
 * After a source aspect commits, moves the entity refresh marker forward for each source aspect
 * whose watched paths changed. The marker moves through a {@code semanticContent} JSON patch that
 * is ingested on its own, so the source write never waits on it and never fails because of it.
 * {@link SemanticRefreshMarkerHook} then decides {@code current} on that write.
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
    Map<String, Long> changedTimes = new LinkedHashMap<>();
    AuditStamp latest = null;
    for (MCLItem source : sources) {
      AuditStamp auditStamp = source.getAuditStamp();
      if (auditStamp == null) {
        continue;
      }
      Set<String> watchKeys =
          changedWatchKeys(urn.getEntityType(), source, content.getEmbeddings().keySet());
      if (watchKeys.isEmpty()) {
        continue;
      }
      watchKeys.forEach(key -> changedTimes.merge(key, auditStamp.getTime(), Math::max));
      if (latest == null || auditStamp.getTime() > latest.getTime()) {
        latest = auditStamp;
      }
    }
    if (changedTimes.isEmpty()) {
      return null;
    }
    SemanticContentPatchBuilder builder = new SemanticContentPatchBuilder().urn(urn);
    changedTimes.forEach(builder::setRefreshSource);
    return PatchItemImpl.builder()
        .build(builder.build(), latest, retrieverContext.getAspectRetriever().getEntityRegistry());
  }

  /** Marker keys of the watched paths this source write changed, across the entity's profiles. */
  private Set<String> changedWatchKeys(
      @Nonnull String entityType, @Nonnull MCLItem source, @Nonnull Set<String> profileKeys) {
    DataMap before = data(source.getPreviousRecordTemplate());
    DataMap after =
        source.getChangeType() == ChangeType.DELETE ? null : data(source.getRecordTemplate());
    Set<String> keys = new HashSet<>();
    for (String key : profileKeys) {
      ContentBinding binding = registry.bindingFor(entityType, key);
      keys.addAll(
          SemanticProfileRefresh.changedWatchKeys(binding, source.getAspectName(), before, after));
    }
    return keys;
  }

  @Nullable
  private static DataMap data(@Nullable RecordTemplate record) {
    return record == null ? null : record.data();
  }
}
