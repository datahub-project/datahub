package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.mxe.SystemMetadata;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
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
      CorpUserInfo proposed = resolveProposedCorpUserInfo(item, current);
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

      Urn authActor = resolveAuthorizingActor(item, operationContext);
      if (isIntrinsicSystemActor(authActor)) {
        continue;
      }

      CorpUserInfo actorInfo = loadCorpUserInfo(operationContext, aspectRetriever, authActor);
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

  /**
   * Session actor that initiated the write (audit stamp), not {@link
   * OperationFingerprint#getActor()} which may reflect system escalation when {@code
   * allowSystemAuthentication} is enabled on the operation context.
   */
  @Nonnull
  private static Urn resolveAuthorizingActor(
      @Nonnull BatchItem item, @Nonnull OperationFingerprint operationContext) {
    AuditStamp itemAudit = item.getAuditStamp();
    if (itemAudit != null && itemAudit.hasActor()) {
      return itemAudit.getActor();
    }
    AuditStamp contextAudit = operationContext.getAuditStamp();
    if (contextAudit != null && contextAudit.hasActor()) {
      return contextAudit.getActor();
    }
    return operationContext.getActor();
  }

  /** Service principal {@link Constants#SYSTEM_ACTOR} has no {@link CorpUserInfo} aspect. */
  private static boolean isIntrinsicSystemActor(@Nonnull Urn authActor) {
    return SYSTEM_ACTOR.equals(authActor.toString());
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
      @Nonnull BatchItem item, @Nullable CorpUserInfo current) {
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
      // Apply the full JsonPatch (add/replace/move/copy) so privilege escalation via copy/move
      // cannot bypass the check. Nest-overlay of add/replace alone is insufficient for auth.
      CorpUserInfo base =
          current != null ? new CorpUserInfo(new DataMap(current.data())) : new CorpUserInfo();
      JsonPatch patch = PatchOperationUtils.resolveJsonPatch((MCPItem) item);
      if (patch == null) {
        return failClosedEscalatingProposal(base);
      }
      try (JsonReader reader =
          Json.createReader(new StringReader(RecordUtils.toJsonString(base)))) {
        JsonObject patched = patch.apply(reader.readObject());
        return RecordUtils.toRecordTemplate(CorpUserInfo.class, patched.toString());
      } catch (RuntimeException e) {
        log.warn(
            "Unable to apply corpUserInfo PATCH for privilege check on {}; failing closed: {}",
            item.getUrn(),
            e.toString());
        return failClosedEscalatingProposal(base);
      }
    }
    return item.getAspect(CorpUserInfo.class);
  }

  /**
   * When the patch cannot be evaluated, assume privileged flags are being set so the authorization
   * check still runs (fail closed for this security control).
   */
  @Nonnull
  private static CorpUserInfo failClosedEscalatingProposal(@Nonnull CorpUserInfo base) {
    CorpUserInfo out = new CorpUserInfo(new DataMap(base.data()));
    out.setSystem(true);
    out.data().put(IS_SUPPORT_USER_FIELD, true);
    return out;
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
