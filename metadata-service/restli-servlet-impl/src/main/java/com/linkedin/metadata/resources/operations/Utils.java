package com.linkedin.metadata.resources.operations;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.resources.restli.RestliUtils.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class Utils {

  private Utils() {}

  public static String restoreIndices(
      @Nonnull String aspectName,
      @Nullable String urn,
      @Nullable String urnLike,
      @Nullable Integer start,
      @Nullable Integer batchSize,
      @Nonnull Authorizer authorizer,
      @Nonnull EntityService entityService) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    EntitySpec resourceSpec = null;
    if (StringUtils.isNotBlank(urn)) {
      Urn resource = UrnUtils.getUrn(urn);
      resourceSpec = new EntitySpec(resource.getEntityType(), resource.toString());
    }
    if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
        && !isAuthorized(
            authentication,
            authorizer,
            ImmutableList.of(PoliciesConfig.RESTORE_INDICES_PRIVILEGE),
            resourceSpec)) {
      throw new RestLiServiceException(
          HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to restore indices.");
    }
    RestoreIndicesArgs args =
        new RestoreIndicesArgs()
            .setAspectName(aspectName)
            .setUrnLike(urnLike)
            .setUrn(urn)
            .setStart(start)
            .setBatchSize(batchSize);
    Map<String, Object> result = new HashMap<>();
    result.put("args", args);
    result.put("result", entityService.restoreIndices(args, log::info));
    return result.toString();
  }
}
