package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.PatchMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.mxe.SystemMetadata;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Blocks privilege escalation on {@link CorpUserInfo} flags used for actor classification:
 *
 * <ul>
 *   <li>{@code system=true} — only existing system users (or trusted internal writes)
 *   <li>{@code isSupportUser=true} — system users, support users, or trusted internal writes
 * </ul>
 */
@Setter
@Getter
@Accessors(chain = true)
public class CorpUserPrivilegedFlagsValidator extends AspectPayloadValidator {

  static final String IS_SUPPORT_USER_FIELD = "isSupportUser";

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    for (BatchItem item : mcpItems) {
      if (!CORP_USER_INFO_ASPECT_NAME.equals(item.getAspectName())) {
        continue;
      }

      CorpUserInfo current = loadCorpUserInfo(operationContext, aspectRetriever, item.getUrn());
      CorpUserInfo proposed = resolveProposedCorpUserInfo(item, current, aspectRetriever);
      if (proposed == null) {
        continue;
      }

      boolean escalatingSystem = isEscalatingToTrue(proposed.isSystem(), isSystemUser(current));
      boolean escalatingSupport =
          isEscalatingToTrue(isSupportUser(proposed), isSupportUser(current));

      if (!escalatingSystem && !escalatingSupport) {
        continue;
      }

      if (isTrustedInternalWrite(item)) {
        continue;
      }

      CorpUserInfo actorInfo =
          loadCorpUserInfo(operationContext, aspectRetriever, operationContext.getActor());
      if (actorInfo == null) {
        exceptions.addException(
            authFailure(item, "Unauthorized to modify privileged CorpUserInfo flags."));
        continue;
      }

      if (escalatingSystem && !isSystemUser(actorInfo)) {
        exceptions.addException(
            authFailure(item, "Only system users may set CorpUserInfo.system to true."));
      }
      if (escalatingSupport && !isSystemUser(actorInfo) && !isSupportUser(actorInfo)) {
        exceptions.addException(
            authFailure(
                item, "Only system or support users may set CorpUserInfo.isSupportUser to true."));
      }
    }

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Nullable
  private static CorpUserInfo resolveProposedCorpUserInfo(
      @Nonnull BatchItem item,
      @Nullable CorpUserInfo current,
      @Nonnull AspectRetriever aspectRetriever) {
    if (item instanceof PatchMCP patchItem) {
      RecordTemplate currentTemplate = current != null ? current : new CorpUserInfo();
      ChangeMCP patched = patchItem.applyPatch(currentTemplate, aspectRetriever);
      return patched.getAspect(CorpUserInfo.class);
    }
    return item.getAspect(CorpUserInfo.class);
  }

  @Nullable
  private static CorpUserInfo loadCorpUserInfo(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Urn urn) {
    if (!"corpuser".equals(urn.getEntityType())) {
      return null;
    }
    Aspect aspect =
        aspectRetriever.getLatestAspectObject(operationContext, urn, CORP_USER_INFO_ASPECT_NAME);
    if (aspect == null) {
      return null;
    }
    return new CorpUserInfo(aspect.data());
  }

  private static boolean isEscalatingToTrue(boolean proposed, boolean current) {
    return proposed && !current;
  }

  public static boolean isSystemUser(@Nullable CorpUserInfo info) {
    return info != null && info.isSystem();
  }

  public static boolean isSupportUser(@Nullable CorpUserInfo info) {
    return info != null
        && info.data().containsKey(IS_SUPPORT_USER_FIELD)
        && Boolean.TRUE.equals(info.data().getBoolean(IS_SUPPORT_USER_FIELD));
  }

  private static boolean isTrustedInternalWrite(@Nonnull BatchItem item) {
    SystemMetadata systemMetadata = item.getSystemMetadata();
    if (systemMetadata != null
        && systemMetadata.hasProperties()
        && SYSTEM_UPDATE_SOURCE.equals(systemMetadata.getProperties().get(APP_SOURCE))) {
      return true;
    }
    AuditStamp auditStamp = item.getAuditStamp();
    return auditStamp != null
        && auditStamp.hasActor()
        && SYSTEM_ACTOR.equals(auditStamp.getActor().toString());
  }

  private static AspectValidationException authFailure(
      @Nonnull BatchItem item, @Nonnull String message) {
    return AspectValidationException.forAuth(item, message);
  }
}
