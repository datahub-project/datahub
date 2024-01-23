package com.linkedin.metadata.resources.platform;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.resources.restli.RestliUtils.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.opentelemetry.extension.annotations.WithSpan;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

/** DataHub Platform Actions */
@Slf4j
@RestLiCollection(name = "platform", namespace = "com.linkedin.platform")
public class PlatformResource extends CollectionResourceTaskTemplate<String, Entity> {

  private static final String ACTION_PRODUCE_PLATFORM_EVENT = "producePlatformEvent";

  @Inject
  @Named("kafkaEventProducer")
  private EventProducer _eventProducer;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

  @Action(name = ACTION_PRODUCE_PLATFORM_EVENT)
  @Nonnull
  @WithSpan
  public Task<Void> producePlatformEvent(
      @ActionParam("name") @Nonnull String eventName,
      @ActionParam("key") @Optional String key,
      @ActionParam("event") @Nonnull PlatformEvent event) {
    Authentication auth = AuthenticationContext.getAuthentication();
    if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
        && !isAuthorized(
            auth,
            _authorizer,
            ImmutableList.of(PoliciesConfig.PRODUCE_PLATFORM_EVENT_PRIVILEGE),
            (EntitySpec) null)) {
      throw new RestLiServiceException(
          HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to produce platform events.");
    }
    log.info(String.format("Emitting platform event. name: %s, key: %s", eventName, key));
    return RestliUtil.toTask(
        () -> {
          _eventProducer.producePlatformEvent(eventName, key, event);
          return null;
        });
  }
}
