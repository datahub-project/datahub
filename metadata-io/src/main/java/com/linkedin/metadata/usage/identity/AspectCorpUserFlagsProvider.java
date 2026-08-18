package com.linkedin.metadata.usage.identity;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.SystemAspect;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Loads CorpUserInfo flags via the session {@link OperationContext} aspect retriever, which
 * delegates to {@link com.linkedin.entity.client.SystemEntityClient} and honors the GMS {@code
 * entityClient} response cache configured in application.yaml. No separate TTL cache in this
 * provider — rely on the per-request {@link UsageActorClassResolver} corp-user flag cache instead.
 */
@Slf4j
public class AspectCorpUserFlagsProvider implements CorpUserFlagsProvider {

  @Override
  public boolean isSystemCorpUser(@Nonnull String corpUserUrn) {
    return false;
  }

  @Override
  public boolean isSupportUser(@Nonnull String corpUserUrn) {
    return false;
  }

  @Override
  @Nonnull
  public CorpUserFlags resolveWithContext(
      @Nonnull OperationContext opContext, @Nonnull String corpUserUrn) {
    return loadFromAspect(opContext, corpUserUrn);
  }

  private static CorpUserFlags loadFromAspect(
      @Nonnull OperationContext opContext, @Nonnull String corpUserUrn) {
    try {
      Urn urn = UrnUtils.getUrn(corpUserUrn);
      if (!"corpuser".equals(urn.getEntityType())) {
        return CorpUserFlags.DEFAULT;
      }
      SystemAspect systemAspect =
          opContext
              .getAspectRetriever()
              .getLatestSystemAspect(opContext, urn, Constants.CORP_USER_INFO_ASPECT_NAME);
      if (systemAspect == null) {
        return CorpUserFlags.DEFAULT;
      }
      CorpUserInfo info = systemAspect.getAspect(CorpUserInfo.class);
      if (info == null) {
        return CorpUserFlags.DEFAULT;
      }
      boolean system = info.isSystem();
      boolean support =
          info.data().containsKey("isSupportUser")
              && Boolean.TRUE.equals(info.data().getBoolean("isSupportUser"));
      return new CorpUserFlags(system, support);
    } catch (Exception e) {
      log.debug("Failed to load CorpUserInfo for {} — fail-open to regular", corpUserUrn, e);
      return CorpUserFlags.DEFAULT;
    }
  }
}
