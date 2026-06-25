package auth.proxy;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles just-in-time provisioning of users authenticated via a trusted reverse proxy. Mirrors the
 * provisioning logic in the OIDC callback path.
 */
public class ProxyAuthProvisioner {

  private static final Logger logger = LoggerFactory.getLogger(ProxyAuthProvisioner.class);

  private final SystemEntityClient systemEntityClient;
  private final OperationContext systemOperationContext;

  @Inject
  public ProxyAuthProvisioner(
      SystemEntityClient systemEntityClient,
      @Named("systemOperationContext") OperationContext systemOperationContext) {
    this.systemEntityClient = systemEntityClient;
    this.systemOperationContext = systemOperationContext;
  }

  public void provisionUser(CorpuserUrn corpUserUrn, String displayName) {
    try {
      final Entity corpUser = systemEntityClient.get(systemOperationContext, corpUserUrn);
      final CorpUserSnapshot existingSnapshot = corpUser.getValue().getCorpUserSnapshot();
      if (existingSnapshot.getAspects().size() > 1) {
        setUserStatusActive(corpUserUrn);
        return;
      }
    } catch (RemoteInvocationException e) {
      logger.debug("User {} not found, will provision", corpUserUrn);
    }

    final CorpUserInfo userInfo = new CorpUserInfo();
    userInfo.setActive(true);
    userInfo.setDisplayName(displayName, SetMode.IGNORE_NULL);
    userInfo.setEmail(corpUserUrn.getId(), SetMode.IGNORE_NULL);

    final CorpUserSnapshot snapshot = new CorpUserSnapshot();
    snapshot.setUrn(corpUserUrn);
    final CorpUserAspectArray aspects = new CorpUserAspectArray();
    aspects.add(CorpUserAspect.create(userInfo));
    snapshot.setAspects(aspects);

    try {
      final Entity entity = new Entity();
      entity.setValue(Snapshot.create(snapshot));
      systemEntityClient.update(systemOperationContext, entity);
      setUserStatusActive(corpUserUrn);
      logger.info("Provisioned proxy-authenticated user {}", corpUserUrn);
    } catch (RemoteInvocationException e) {
      logger.error("Failed to provision proxy-authenticated user {}", corpUserUrn, e);
    }
  }

  private void setUserStatusActive(CorpuserUrn corpUserUrn) {
    try {
      final CorpUserStatus status = new CorpUserStatus();
      status.setStatus(Constants.CORP_USER_STATUS_ACTIVE);
      status.setLastModified(
          new AuditStamp()
              .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis()));
      final MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(corpUserUrn);
      proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
      proposal.setAspectName(Constants.CORP_USER_STATUS_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(status));
      proposal.setChangeType(ChangeType.UPSERT);
      systemEntityClient.ingestProposal(systemOperationContext, proposal);
    } catch (Exception e) {
      logger.error("Failed to set user status active for {}", corpUserUrn, e);
    }
  }
}
