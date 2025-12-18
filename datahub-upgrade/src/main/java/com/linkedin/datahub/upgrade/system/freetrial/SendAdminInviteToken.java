package com.linkedin.datahub.upgrade.system.freetrial;

import com.datahub.authentication.invite.InviteTokenService;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ControlPlaneService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * System upgrade that sends the admin invite token to the control plane, so it can redirect to
 * signup page.
 */
@Slf4j
public class SendAdminInviteToken implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public SendAdminInviteToken(
      OperationContext opContext,
      EntityService<?> entityService,
      InviteTokenService inviteTokenService,
      ControlPlaneService controlPlaneService,
      boolean enabled,
      int retryCount,
      int retryIntervalSeconds,
      boolean reprocessEnabled) {

    if (enabled && controlPlaneService.isConfigured()) {
      steps =
          ImmutableList.of(
              new SendAdminInviteTokenStep(
                  opContext,
                  entityService,
                  inviteTokenService,
                  controlPlaneService,
                  enabled,
                  retryCount,
                  retryIntervalSeconds,
                  reprocessEnabled));
    } else {
      steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
