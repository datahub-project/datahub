package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.persistAspect;

import com.linkedin.common.DisplayProperties;
import com.linkedin.common.IconLibrary;
import com.linkedin.common.IconProperties;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DisplayPropertiesUpdateInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateDisplayPropertiesResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final DisplayPropertiesUpdateInput input =
        bindArgument(environment.getArgument("input"), DisplayPropertiesUpdateInput.class);
    final String urn = bindArgument(environment.getArgument("urn"), String.class);

    final QueryContext context = environment.getContext();
    Urn targetUrn = Urn.createFromString(urn);

    log.info(
        "Updating display properties. urn: {} input: {}", targetUrn.toString(), input.toString());

    if (!_entityService.exists(context.getOperationContext(), targetUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", targetUrn, targetUrn));
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // displayProperties is shared across domains and glossary entities. Authorize
          // based on the target entity type so a glossary editor can change a term/node
          // color without also having MANAGE_DOMAINS, and vice versa.
          final String entityType = targetUrn.getEntityType();
          final boolean authorized;
          if (Constants.GLOSSARY_TERM_ENTITY_NAME.equals(entityType)
              || Constants.GLOSSARY_NODE_ENTITY_NAME.equals(entityType)) {
            authorized =
                GlossaryUtils.canManageGlossaries(context)
                    || GlossaryUtils.canUpdateGlossaryEntity(targetUrn, context, _entityClient);
          } else {
            authorized = AuthorizationUtils.canManageDomains(context);
          }
          if (!authorized) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            DisplayProperties existingDisplayProperties =
                (DisplayProperties)
                    EntityUtils.getAspectFromEntity(
                        context.getOperationContext(),
                        targetUrn.toString(),
                        Constants.DISPLAY_PROPERTIES_ASPECT_NAME,
                        _entityService,
                        new DisplayProperties());
            if (input.getColorHex() != null) {
              existingDisplayProperties.setColorHex(input.getColorHex());
            }
            if (input.getIcon() != null) {
              if (!existingDisplayProperties.hasIcon()) {
                existingDisplayProperties.setIcon(new IconProperties());
              }
              if (input.getIcon().getName() != null) {
                existingDisplayProperties.getIcon().setName(input.getIcon().getName());
              }
              if (input.getIcon().getStyle() != null) {
                existingDisplayProperties.getIcon().setStyle(input.getIcon().getStyle());
              }
              if (input.getIcon().getIconLibrary() != null) {
                existingDisplayProperties
                    .getIcon()
                    .setIconLibrary(
                        IconLibrary.valueOf(input.getIcon().getIconLibrary().toString()));
              }
            }
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            persistAspect(
                context.getOperationContext(),
                targetUrn,
                Constants.DISPLAY_PROPERTIES_ASPECT_NAME,
                existingDisplayProperties,
                actor,
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update DisplayProperties for urn: {}, properties: {}. {}",
                targetUrn.toString(),
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to update DisplayProperties for urn: %s, properties: %s. %s",
                    targetUrn.toString(), input.toString(), e.getMessage()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
