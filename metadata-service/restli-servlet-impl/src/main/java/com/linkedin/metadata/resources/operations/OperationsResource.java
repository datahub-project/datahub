package com.linkedin.metadata.resources.operations;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import com.linkedin.timeseries.TimeseriesIndexSizeResultArray;
import com.linkedin.timeseries.TimeseriesIndicesSizesResult;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;
import static com.linkedin.metadata.resources.restli.RestliUtils.*;


/**
 * Endpoints for performing maintenance operations
 */
@RestLiCollection(name = "operations", namespace = "com.linkedin.operations")
public class OperationsResource extends CollectionResourceTaskTemplate<String, VersionedAspect> {
  private static final String ACTION_GET_INDEX_SIZES = "getIndexSizes";
  public static final String ACTION_RESTORE_INDICES = "restoreIndices";
  private static final String PARAM_ASPECT = "aspect";

  @Inject
  @Named("entityService")
  private EntityService _entityService;
  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

  @Action(name = ACTION_RESTORE_INDICES)
  @Nonnull
  @WithSpan
  public Task<String> restoreIndices(@ActionParam(PARAM_ASPECT) @Optional @Nonnull String aspectName,
      @ActionParam(PARAM_URN) @Optional @Nullable String urn,
      @ActionParam(PARAM_URN_LIKE) @Optional @Nullable String urnLike,
      @ActionParam("start") @Optional @Nullable Integer start,
      @ActionParam("batchSize") @Optional @Nullable Integer batchSize
  ) {
    return RestliUtil.toTask(() -> {
      return Utils.restoreIndices(aspectName, urn, urnLike, start, batchSize, _authorizer, _entityService);
    }, MetricRegistry.name(this.getClass(), "restoreIndices"));
  }

  @Action(name = ACTION_GET_INDEX_SIZES)
  @Nonnull
  @WithSpan
  public Task<TimeseriesIndicesSizesResult> getIndexSizes() {
    return RestliUtil.toTask(() -> {
      Authentication authentication = AuthenticationContext.getAuthentication();
      if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
          && !isAuthorized(authentication, _authorizer, ImmutableList.of(PoliciesConfig.GET_TIMESERIES_INDEX_SIZES_PRIVILEGE),
          List.of(java.util.Optional.empty()))) {
        throw new RestLiServiceException(HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to get index sizes.");
      }
      TimeseriesIndicesSizesResult result = new TimeseriesIndicesSizesResult();
      result.setIndexSizes(new TimeseriesIndexSizeResultArray(_timeseriesAspectService.getIndexSizes()));
      return result;
    }, MetricRegistry.name(this.getClass(), "getIndexSizes"));
  }
}
