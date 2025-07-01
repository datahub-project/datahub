package com.linkedin.metadata.resources.platform;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.datahub.authorization.AuthUtil.isAPIOperationsAuthorized;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.authorization.Disjunctive;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.resources.restli.RestliUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Collectors;

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

  @Inject
  @Named("systemOperationContext")
  private OperationContext systemOperationContext;

  @Action(name = ACTION_PRODUCE_PLATFORM_EVENT)
  @Nonnull
  @WithSpan
  public Task<Void> producePlatformEvent(
      @ActionParam("name") @Nonnull String eventName,
      @ActionParam("key") @Optional String key,
      @ActionParam("event") @Nonnull PlatformEvent event) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    ACTION_PRODUCE_PLATFORM_EVENT), _authorizer,
            auth, true);

    if (!isAPIOperationsAuthorized(
            opContext,
            PoliciesConfig.PRODUCE_PLATFORM_EVENT_PRIVILEGE)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to produce platform events.");
    }
    log.info(String.format("Emitting platform event. name: %s, key: %s", eventName, key));
    return RestliUtils.toTask(
        () -> {
          _eventProducer.producePlatformEvent(eventName, key, event);
          return null;
        });
  }
}
