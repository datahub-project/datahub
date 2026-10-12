package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
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
import com.linkedin.versionset.VersionSetProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
    List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : items) {
      VersionProperties current =
          toVersionProperties(
              aspectRetriever.getLatestAspectObject(
                  operationContext, item.getUrn(), VERSION_PROPERTIES_ASPECT_NAME));
      VersionProperties proposed = resolveProposed(item, current, aspectRetriever);
      if (proposed == null || !proposed.hasVersionSet()) {
        continue;
      }
      Urn versionSetUrn = proposed.getVersionSet();
      VersionSetProperties versionSet =
          toVersionSetProperties(
              aspectRetriever.getLatestAspectObject(
                  operationContext, versionSetUrn, VERSION_SET_PROPERTIES_ASPECT_NAME));

      Urn demotedLatest = null;
      boolean writesVersionSet = versionSet == null;
      if (versionSet != null && !item.getUrn().equals(versionSet.getLatest())) {
        // Mirrors VersionPropertiesSideEffect: a higher sortId becomes the new latest.
        VersionProperties previousLatest =
            toVersionProperties(
                aspectRetriever.getLatestAspectObject(
                    operationContext, versionSet.getLatest(), VERSION_PROPERTIES_ASPECT_NAME));
        if (previousLatest == null) {
          writesVersionSet = true;
        } else if (proposed.getSortId().compareTo(previousLatest.getSortId()) > 0) {
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

  @Nullable
  private static VersionProperties toVersionProperties(@Nullable Aspect aspect) {
    return aspect == null
        ? null
        : RecordUtils.toRecordTemplate(VersionProperties.class, aspect.data());
  }

  @Nullable
  private static VersionSetProperties toVersionSetProperties(@Nullable Aspect aspect) {
    return aspect == null
        ? null
        : RecordUtils.toRecordTemplate(VersionSetProperties.class, aspect.data());
  }
}
