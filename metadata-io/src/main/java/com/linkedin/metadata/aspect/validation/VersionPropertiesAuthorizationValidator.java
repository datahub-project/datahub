package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.entity.versioning.sideeffects.VersionPropertiesSideEffect;
import com.linkedin.versionset.VersionSetProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Writing {@code versionProperties} links an entity into a version set, and {@code
 * VersionPropertiesSideEffect} then rewrites the version set's latest pointer and the previous
 * latest version's {@code isLatest} flag. Ingest-time authorization only covers the entity being
 * written, so this validator requires {@code EDIT_ENTITY} on the version set (as the {@code
 * linkAssetVersion} mutation does) and on the previous latest version when it would be demoted.
 * Re-ingesting an unchanged link that touches nothing else needs no extra privileges.
 */
@Setter
@Getter
@Accessors(chain = true)
public class VersionPropertiesAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    // Reads are batched per phase (stored aspect -> version set -> previous latest) so a batch of
    // N items costs at most three primary-store lookups per entity type, not 3N.
    Map<Urn, VersionProperties> stored =
        read(
            operationContext,
            aspectRetriever,
            items.stream().map(BatchItem::getUrn).collect(Collectors.toSet()),
            VERSION_PROPERTIES_ASPECT_NAME,
            VersionProperties.class);
    List<Map.Entry<BatchItem, VersionProperties>> proposals = new ArrayList<>();
    for (BatchItem item : items) {
      VersionProperties proposed =
          resolveProposed(item, stored.get(item.getUrn()), aspectRetriever);
      if (proposed != null && proposed.hasVersionSet()) {
        proposals.add(Map.entry(item, proposed));
      }
    }
    if (proposals.isEmpty()) {
      return List.of();
    }
    Map<Urn, VersionSetProperties> versionSets =
        read(
            operationContext,
            aspectRetriever,
            proposals.stream().map(e -> e.getValue().getVersionSet()).collect(Collectors.toSet()),
            VERSION_SET_PROPERTIES_ASPECT_NAME,
            VersionSetProperties.class);
    Map<Urn, VersionProperties> latestVersions =
        read(
            operationContext,
            aspectRetriever,
            versionSets.values().stream()
                .map(VersionSetProperties::getLatest)
                .collect(Collectors.toSet()),
            VERSION_PROPERTIES_ASPECT_NAME,
            VersionProperties.class);

    List<AspectValidationException> failures = new ArrayList<>();
    for (Map.Entry<BatchItem, VersionProperties> entry : proposals) {
      BatchItem item = entry.getKey();
      VersionProperties proposed = entry.getValue();
      VersionProperties current = stored.get(item.getUrn());
      Urn versionSetUrn = proposed.getVersionSet();
      VersionSetProperties versionSet = versionSets.get(versionSetUrn);

      Urn demotedLatest = null;
      boolean writesVersionSet = versionSet == null;
      if (versionSet != null && !item.getUrn().equals(versionSet.getLatest())) {
        VersionProperties previousLatest = latestVersions.get(versionSet.getLatest());
        if (previousLatest == null) {
          writesVersionSet = true;
        } else if (VersionPropertiesSideEffect.supersedes(proposed, previousLatest)) {
          writesVersionSet = true;
          demotedLatest = versionSet.getLatest();
        }
      }
      boolean linksVersionSet =
          current == null || !Objects.equals(current.getVersionSet(), versionSetUrn);

      if ((linksVersionSet || writesVersionSet) && !canEdit(session, versionSetUrn)) {
        failures.add(
            authFailure(item, "Unauthorized to modify version set: " + versionSetUrn.toString()));
      }
      if (demotedLatest != null && !canEdit(session, demotedLatest)) {
        failures.add(
            authFailure(
                item,
                "Unauthorized to replace the latest version of "
                    + versionSetUrn
                    + ": "
                    + demotedLatest));
      }
    }
    return failures;
  }

  private static boolean canEdit(@Nonnull AuthorizationSession session, @Nonnull Urn urn) {
    return AuthUtil.isAuthorizedEntityUrns(session, ApiOperation.UPDATE, Set.of(urn));
  }

  @Nullable
  private static VersionProperties resolveProposed(
      @Nonnull BatchItem item,
      @Nullable VersionProperties current,
      @Nonnull AspectRetriever aspectRetriever) {
    if (!ChangeType.PATCH.equals(item.getChangeType())) {
      return item.getAspect(VersionProperties.class);
    }
    // A PATCH arrives as a PatchItemImpl by default, or as a ProposedItem under alternate MCP
    // validation. Neither carries the resulting aspect, so apply it here. current may be null: a
    // generic JSON patch can create the aspect from scratch.
    PatchItemImpl patchItem =
        item instanceof PatchItemImpl
            ? (PatchItemImpl) item
            : PatchItemImpl.builder()
                .build(
                    ((ProposedItem) item).getMetadataChangeProposal(),
                    item.getAuditStamp(),
                    aspectRetriever.getEntityRegistry());
    try {
      return patchItem.applyPatch(current, aspectRetriever).getAspect(VersionProperties.class);
    } catch (RuntimeException e) {
      // A patch that cannot be applied fails the write downstream; nothing to authorize.
      return null;
    }
  }

  @Nonnull
  private static <T extends RecordTemplate> Map<Urn, T> read(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Set<Urn> urns,
      @Nonnull String aspectName,
      @Nonnull Class<T> clazz) {
    Map<Urn, T> result = new HashMap<>();
    AspectRetriever.getLatestAspectObjectsAcrossEntityTypes(
            aspectRetriever, operationContext, urns, Set.of(aspectName))
        .forEach(
            (urn, aspects) -> {
              Aspect aspect = aspects.get(aspectName);
              if (aspect != null) {
                result.put(urn, RecordUtils.toRecordTemplate(clazz, aspect.data()));
              }
            });
    return result;
  }
}
