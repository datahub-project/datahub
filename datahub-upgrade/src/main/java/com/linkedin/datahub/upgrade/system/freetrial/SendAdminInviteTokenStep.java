package com.linkedin.datahub.upgrade.system.freetrial;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ControlPlaneService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Upgrade step to send the admin invite token to the control plane.
 *
 * <p>This step retrieves the admin invite token from InviteTokenService and sends it to the control
 * plane API with generous retry logic. This allows the control plane to provision new trial
 * instances with the correct admin token.
 */
@Slf4j
public class SendAdminInviteTokenStep implements UpgradeStep {

  private static final String UPGRADE_ID = "SendAdminInviteToken";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
  private static final String ADMIN_ROLE_URN = "urn:li:dataHubRole:Admin";

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final InviteTokenService inviteTokenService;
  private final ControlPlaneService controlPlaneService;
  private final boolean enabled;
  private final int retryCount;
  private final int retryIntervalSeconds;
  private final boolean reprocessEnabled;

  public SendAdminInviteTokenStep(
      @Nonnull OperationContext systemOpContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull InviteTokenService inviteTokenService,
      @Nonnull ControlPlaneService controlPlaneService,
      boolean enabled,
      int retryCount,
      int retryIntervalSeconds,
      boolean reprocessEnabled) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.inviteTokenService = inviteTokenService;
    this.controlPlaneService = controlPlaneService;
    this.enabled = enabled;
    this.retryCount = retryCount;
    this.retryIntervalSeconds = retryIntervalSeconds;
    this.reprocessEnabled = reprocessEnabled;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        log.info("Starting SendAdminInviteToken upgrade step");

        String adminInviteToken =
            inviteTokenService.getEncryptedInviteToken(context.opContext(), ADMIN_ROLE_URN, false);

        if (adminInviteToken.isEmpty()) {
          log.error("Failed to get admin invite token from InviteTokenService");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        log.info("Retrieved admin invite token, sending to control plane");

        // Send to control plane with retry logic
        boolean success =
            controlPlaneService.sendAdminInviteToken(
                adminInviteToken, retryCount, retryIntervalSeconds);

        if (!success) {
          log.error("Failed to send admin invite token to control plane");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        // Mark upgrade as complete
        BootstrapStep.setUpgradeResult(systemOpContext, UPGRADE_ID_URN, entityService);

        log.info("Successfully sent admin invite token to control plane");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error("Error sending admin invite token to control plane: {}", e.getMessage(), e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (!enabled) {
      log.info("SendAdminInviteToken is disabled. Skipping.");
      return true;
    }

    if (reprocessEnabled) {
      log.info("SendAdminInviteToken reprocess enabled. Running.");
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            systemOpContext, UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }

    return previouslyRun;
  }
}
