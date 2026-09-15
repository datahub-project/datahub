package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateServiceAccountDefaultViewInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.CorpUserViewsSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for setting or clearing the default view on a service account. Requires the
 * MANAGE_SERVICE_ACCOUNTS platform privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateServiceAccountDefaultViewResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;
  private final EntityClient _entityClient;
  private final ViewService _viewService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateServiceAccountDefaultViewInput input =
        bindArgument(environment.getArgument("input"), UpdateServiceAccountDefaultViewInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canManageServiceAccounts(context)) {
            throw new AuthorizationException(
                "Unauthorized to manage service accounts. "
                    + "Please contact your DataHub administrator.");
          }

          final Urn serviceAccountUrn = UrnUtils.getUrn(input.getUrn());
          validateServiceAccount(context, serviceAccountUrn);

          if (input.getDefaultView() != null) {
            validateGlobalView(context, UrnUtils.getUrn(input.getDefaultView()));
          }

          try {
            final CorpUserSettings settings =
                ServiceAccountUtils.getOrCreateSettings(
                    _settingsService.getCorpUserSettings(
                        context.getOperationContext(), serviceAccountUrn));

            final CorpUserViewsSettings viewsSettings =
                settings.hasViews() ? settings.getViews() : new CorpUserViewsSettings();

            viewsSettings.setDefaultView(
                input.getDefaultView() != null ? UrnUtils.getUrn(input.getDefaultView()) : null,
                SetMode.REMOVE_IF_NULL);

            settings.setViews(viewsSettings);

            _settingsService.updateCorpUserSettings(
                context.getOperationContext(), serviceAccountUrn, settings);

            return true;
          } catch (AuthorizationException | IllegalArgumentException e) {
            throw e;
          } catch (Exception e) {
            log.error(
                "Failed to update default view for service account {}: {}",
                input.getUrn(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to update default view for service account %s", input.getUrn()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateServiceAccount(
      @Nonnull QueryContext context, @Nonnull Urn serviceAccountUrn) {
    try {
      final EntityResponse response =
          _entityClient.getV2(
              context.getOperationContext(),
              Constants.CORP_USER_ENTITY_NAME,
              serviceAccountUrn,
              Set.of(Constants.SUB_TYPES_ASPECT_NAME));

      if (!ServiceAccountUtils.isServiceAccount(response)) {
        throw new IllegalArgumentException(
            String.format("URN %s is not a service account.", serviceAccountUrn));
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to validate service account %s", serviceAccountUrn), e);
    }
  }

  /** Only global (public) views may be set as a service account default. */
  private void validateGlobalView(@Nonnull QueryContext context, @Nonnull Urn viewUrn) {
    final DataHubViewInfo viewInfo =
        _viewService.getViewInfo(context.getOperationContext(), viewUrn);
    if (viewInfo == null) {
      throw new IllegalArgumentException(String.format("View %s does not exist.", viewUrn));
    }
    if (!DataHubViewType.GLOBAL.equals(viewInfo.getType())) {
      throw new IllegalArgumentException(
          "Only public (global) views can be set as the default view for a service account. "
              + "Personal views are not supported.");
    }
  }
}
