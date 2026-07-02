package auth.proxy;

import static auth.AuthUtils.createActorCookie;
import static auth.AuthUtils.createSessionMap;
import static play.mvc.Results.internalServerError;
import static utils.FrontendConstants.PROXY_LOGIN;

import auth.CookieConfigs;
import auth.ProxyConfigs;
import client.AuthServiceClient;
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
import com.typesafe.config.Config;
import io.datahubproject.metadata.context.OperationContext;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

/**
 * Handles authentication of users via a trusted reverse proxy. Mirrors the callback logic in the
 * OIDC path.
 */
public class ProxyCallbackLogic {

  private static final Logger logger = LoggerFactory.getLogger(ProxyCallbackLogic.class);

  private static final String AUTH_VERBOSE_LOGGING = "auth.verbose.logging";

  private final SystemEntityClient systemEntityClient;
  private final OperationContext systemOperationContext;
  private final AuthServiceClient authClient;
  private final CookieConfigs cookieConfigs;
  private final ProxyConfigs proxyConfigs;
  private final boolean verbose;

  @Inject
  public ProxyCallbackLogic(
      SystemEntityClient systemEntityClient,
      @Named("systemOperationContext") OperationContext systemOperationContext,
      AuthServiceClient authClient,
      Config configs) {
    this.systemEntityClient = systemEntityClient;
    this.systemOperationContext = systemOperationContext;
    this.authClient = authClient;
    this.cookieConfigs = new CookieConfigs(configs);
    this.proxyConfigs = new ProxyConfigs(configs);
    this.verbose =
        configs.hasPath(AUTH_VERBOSE_LOGGING) && configs.getBoolean(AUTH_VERBOSE_LOGGING);
  }

  public Result handleProxyLogin(Http.Request request, String redirectPath) {
    final String username = request.header(proxyConfigs.getUserHeader()).get();
    final CorpuserUrn corpUserUrn = new CorpuserUrn(username);

    try {
      if (proxyConfigs.isJitProvisioningEnabled()) {
        tryProvisionUser(systemOperationContext, corpUserUrn, username);
      }
      setUserStatus(
          systemOperationContext,
          corpUserUrn,
          new CorpUserStatus()
              .setStatus(Constants.CORP_USER_STATUS_ACTIVE)
              .setLastModified(
                  new AuditStamp()
                      .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                      .setTime(System.currentTimeMillis())));
    } catch (Exception e) {
      logger.error("Failed to perform post authentication steps. Redirecting to error page.", e);
      return internalServerError(
          String.format(
              "Failed to perform post authentication steps. Error message: %s", e.getMessage()));
    }

    final String accessToken =
        authClient.generateSessionTokenForUser(corpUserUrn.getId(), PROXY_LOGIN);

    if (verbose) {
      logger.info(
          "Proxy auth: authenticated user '{}' via header '{}'",
          username,
          proxyConfigs.getUserHeader());
    }

    return Results.redirect(redirectPath)
        .withSession(createSessionMap(corpUserUrn.toString(), accessToken))
        .withCookies(
            createActorCookie(
                corpUserUrn.toString(),
                cookieConfigs.getTtlInHours(),
                cookieConfigs.getAuthCookieSameSite(),
                cookieConfigs.getAuthCookieSecure()));
  }

  private void tryProvisionUser(
      OperationContext opContext, CorpuserUrn corpUserUrn, String displayName) {
    logger.debug("Attempting to provision user with urn {}", corpUserUrn);

    try {
      final Entity corpUser = systemEntityClient.get(opContext, corpUserUrn);
      final CorpUserSnapshot existingSnapshot = corpUser.getValue().getCorpUserSnapshot();

      if (existingSnapshot.getAspects().size() <= 1) {
        logger.debug("User {} does not yet exist. Provisioning...", corpUserUrn);
        final CorpUserInfo userInfo = new CorpUserInfo();
        userInfo.setActive(true);
        userInfo.setDisplayName(displayName, SetMode.IGNORE_NULL);
        userInfo.setEmail(corpUserUrn.getId(), SetMode.IGNORE_NULL);

        final CorpUserSnapshot snapshot = new CorpUserSnapshot();
        snapshot.setUrn(corpUserUrn);
        final CorpUserAspectArray aspects = new CorpUserAspectArray();
        aspects.add(CorpUserAspect.create(userInfo));
        snapshot.setAspects(aspects);

        final Entity newEntity = new Entity();
        newEntity.setValue(Snapshot.create(snapshot));
        systemEntityClient.update(opContext, newEntity);
        logger.debug("Successfully provisioned user {}", corpUserUrn);
      }
      logger.debug("User {} already exists. Skipping provisioning", corpUserUrn);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(
          String.format("Failed to provision user with urn %s.", corpUserUrn), e);
    }
  }

  private void setUserStatus(
      OperationContext opContext, CorpuserUrn corpUserUrn, CorpUserStatus newStatus)
      throws Exception {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(corpUserUrn);
    proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    proposal.setAspectName(Constants.CORP_USER_STATUS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newStatus));
    proposal.setChangeType(ChangeType.UPSERT);
    systemEntityClient.ingestProposal(opContext, proposal);
  }
}
