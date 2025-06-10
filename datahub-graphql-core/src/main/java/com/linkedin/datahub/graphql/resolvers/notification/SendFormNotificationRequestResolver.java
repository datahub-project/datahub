package com.linkedin.datahub.graphql.resolvers.notification;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.SendFormNotificationRequestInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SendFormNotificationRequestResolver
    implements DataFetcher<CompletableFuture<Boolean>> {
  final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final OperationContext opContext = context.getOperationContext();

    final SendFormNotificationRequestInput input =
        bindArgument(environment.getArgument("input"), SendFormNotificationRequestInput.class);
    NotificationRequest notificationRequest =
        FormNotificationRequestUtils.mapFormNotificationRequestInput(input);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (!opContext.isSystemAuth()) {
              // Only system authorization is allowed to send notification request
              throw new Exception("Unauthorized to send notification requests.");
            }

            _entityClient.producePlatformEvent(
                opContext,
                Constants.NOTIFICATION_REQUEST_EVENT_NAME,
                null,
                FormNotificationRequestUtils.createPlatformEvent(notificationRequest));

            return true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to send notification request", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
