package com.datahub.authentication.session;

import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_STATUS_SUSPENDED;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.datahub.authentication.LoginDenialReason;
import com.linkedin.common.Status;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * Evaluates whether a corp user may receive a UI session token (aligned with {@link
 * io.datahubproject.metadata.context.ActorContext#isActive}).
 */
@RequiredArgsConstructor
public class UserSessionEligibilityChecker {

  private static final Set<String> SESSION_ELIGIBILITY_ASPECT_NAMES =
      Set.of(
          CORP_USER_KEY_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          CORP_USER_STATUS_ASPECT_NAME,
          CORP_USER_INFO_ASPECT_NAME);

  private final EntityService<?> _entityService;

  /**
   * @param userIdOrUrn corpuser username or full urn:li:corpuser:... string
   */
  @Nonnull
  public Optional<LoginDenialReason> checkEligibility(
      @Nonnull final OperationContext opContext,
      @Nonnull final String userIdOrUrn,
      final boolean enforceExistenceEnabled) {
    final Urn userUrn = resolveCorpUserUrn(userIdOrUrn);

    final Map<String, RecordTemplate> aspects =
        _entityService.getLatestAspectsForUrn(
            opContext, userUrn, SESSION_ELIGIBILITY_ASPECT_NAMES, false);

    final RecordTemplate keyAspect = aspects.get(CORP_USER_KEY_ASPECT_NAME);
    if (enforceExistenceEnabled && keyAspect == null) {
      return Optional.of(LoginDenialReason.HARD_DELETED);
    }

    final RecordTemplate statusAspect = aspects.get(STATUS_ASPECT_NAME);
    if (statusAspect != null) {
      final Status status = new Status(statusAspect.data());
      if (status.isRemoved()) {
        return Optional.of(LoginDenialReason.SOFT_DELETED);
      }
    }

    final RecordTemplate corpUserStatusAspect = aspects.get(CORP_USER_STATUS_ASPECT_NAME);
    if (corpUserStatusAspect != null) {
      final CorpUserStatus cus = new CorpUserStatus(corpUserStatusAspect.data());
      if (cus.hasStatus() && CORP_USER_STATUS_SUSPENDED.equals(cus.getStatus())) {
        return Optional.of(LoginDenialReason.SUSPENDED);
      }
    }

    final RecordTemplate corpUserInfoAspect = aspects.get(CORP_USER_INFO_ASPECT_NAME);

    if (keyAspect != null && corpUserInfoAspect == null && corpUserStatusAspect == null) {
      return Optional.of(LoginDenialReason.NOT_PROVISIONED);
    }

    return Optional.empty();
  }

  @Nonnull
  private static Urn resolveCorpUserUrn(@Nonnull final String userIdOrUrn) {
    if (userIdOrUrn.startsWith("urn:li:corpuser:")) {
      return UrnUtils.getUrn(userIdOrUrn);
    }
    return UrnUtils.getUrn(new CorpuserUrn(userIdOrUrn).toString());
  }
}
