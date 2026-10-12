package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_FIELD_NAME;
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
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.versionset.VersionSetProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
 *
 * <p>Version sets have opaque URNs and no platform instance, so a principal whose policies are
 * scoped to its own entities can never hold {@code EDIT_ENTITY} on one. Edit on the version set is
 * therefore not required when the caller can edit every current member of it, which includes a set
 * that has no members yet. A set holding anyone else's entity still requires edit on the set.
 */
@Setter
@Getter
@Accessors(chain = true)
public class VersionPropertiesAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  /** Above this many members, edit on the version set itself is required. */
  static final int MAX_MEMBERS_CHECKED = 100;

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

    Map<Urn, Boolean> versionSetAccess = new HashMap<>();
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
        // Mirrors VersionPropertiesSideEffect: a higher sortId becomes the new latest.
        VersionProperties previousLatest = latestVersions.get(versionSet.getLatest());
        if (previousLatest == null) {
          writesVersionSet = true;
        } else if (proposed.getSortId().compareTo(previousLatest.getSortId()) > 0) {
          writesVersionSet = true;
          demotedLatest = versionSet.getLatest();
        }
      }
      boolean linksVersionSet =
          current == null || !Objects.equals(current.getVersionSet(), versionSetUrn);

      if ((linksVersionSet || writesVersionSet)
          && !versionSetAccess.computeIfAbsent(
              versionSetUrn,
              urn ->
                  canEditVersionSet(
                      session,
                      retrieverContext.getSearchRetriever(),
                      urn,
                      item.getUrn().getEntityType(),
                      versionSet))) {
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

  private static boolean canEditVersionSet(
      @Nonnull AuthorizationSession session,
      @Nonnull SearchRetriever searchRetriever,
      @Nonnull Urn versionSetUrn,
      @Nonnull String entityType,
      @Nullable VersionSetProperties versionSet) {
    if (canEdit(session, versionSetUrn)) {
      return true;
    }
    // Membership is only indexed, so a member linked moments ago may be missing here. The stored
    // latest pointer is always added, which keeps the one member this write can modify (the
    // previous latest) consistent.
    ScrollResult result =
        searchRetriever.scroll(
            List.of(entityType),
            QueryUtils.newConjunctiveFilter(
                CriterionUtils.buildCriterion(
                    VERSION_SET_FIELD_NAME, Condition.EQUAL, versionSetUrn.toString())),
            null,
            MAX_MEMBERS_CHECKED + 1,
            List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
            SearchRetriever.RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS);
    Set<Urn> members =
        result.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toCollection(HashSet::new));
    if (versionSet != null) {
      members.add(versionSet.getLatest());
    }
    if (result.getNumEntities() > MAX_MEMBERS_CHECKED || members.size() > MAX_MEMBERS_CHECKED) {
      return false;
    }
    return members.isEmpty()
        || AuthUtil.isAuthorizedEntityUrns(session, ApiOperation.UPDATE, members);
  }

  @Nullable
  private static VersionProperties resolveProposed(
      @Nonnull BatchItem item,
      @Nullable VersionProperties current,
      @Nonnull AspectRetriever aspectRetriever) {
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof ProposedItem) {
      if (current == null) {
        // Patching a missing versionProperties aspect fails downstream; nothing to authorize.
        return null;
      }
      ProposedItem proposedItem = (ProposedItem) item;
      return PatchItemImpl.builder()
          .build(
              proposedItem.getMetadataChangeProposal(),
              proposedItem.getAuditStamp(),
              aspectRetriever.getEntityRegistry())
          .applyPatch(current, aspectRetriever)
          .getAspect(VersionProperties.class);
    }
    return item.getAspect(VersionProperties.class);
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
