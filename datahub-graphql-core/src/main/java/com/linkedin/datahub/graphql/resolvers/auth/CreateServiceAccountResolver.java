package com.linkedin.datahub.graphql.resolvers.auth;

import static com.datahub.authorization.AuthUtil.isAuthorized;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateServiceAccountInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ServiceAccount;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for creating service accounts. Service accounts are stored as CorpUser entities with a
 * naming convention prefix "service_" followed by a UUID, and a SubTypes aspect with typeNames
 * containing "SERVICE_ACCOUNT".
 */
@Slf4j
public class CreateServiceAccountResolver
    implements DataFetcher<CompletableFuture<ServiceAccount>> {

  private final EntityClient _entityClient;

  public CreateServiceAccountResolver(final EntityClient entityClient) {
    this._entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ServiceAccount> get(DataFetchingEnvironment environment)
      throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final CreateServiceAccountInput input =
              bindArgument(environment.getArgument("input"), CreateServiceAccountInput.class);

          if (!isAuthorized(
              context.getOperationContext(), PoliciesConfig.MANAGE_SERVICE_ACCOUNTS_PRIVILEGE)) {
            throw new AuthorizationException(
                "Unauthorized to create service accounts. Please contact your DataHub administrator.");
          }

          try {
            // Generate unique identifiers using UUID
            final String serviceAccountId = ServiceAccountUtils.generateServiceAccountId();
            final String serviceAccountUrnStr = "urn:li:corpuser:" + serviceAccountId;
            final Urn serviceAccountUrn = Urn.createFromString(serviceAccountUrnStr);

            // Build all proposals
            final List<MetadataChangeProposal> proposals = new ArrayList<>();

            // Create the CorpUser key
            final CorpUserKey key = new CorpUserKey();
            key.setUsername(serviceAccountId);
            proposals.add(
                buildProposal(serviceAccountUrn, Constants.CORP_USER_KEY_ASPECT_NAME, key));

            // Create the CorpUserInfo aspect
            final CorpUserInfo info = new CorpUserInfo();
            info.setActive(true);
            // Use display name if provided, otherwise use the generated ID
            info.setDisplayName(
                input.getDisplayName() != null ? input.getDisplayName() : serviceAccountId);
            if (input.getDescription() != null) {
              info.setTitle(input.getDescription());
            }
            proposals.add(
                buildProposal(serviceAccountUrn, Constants.CORP_USER_INFO_ASPECT_NAME, info));

            // Create the SubTypes aspect
            final SubTypes subTypes = new SubTypes();
            subTypes.setTypeNames(new StringArray(ServiceAccountUtils.SERVICE_ACCOUNT_SUB_TYPE));
            proposals.add(
                buildProposal(serviceAccountUrn, Constants.SUB_TYPES_ASPECT_NAME, subTypes));

            log.info(
                "Creating service account {} with {} proposals (SubTypes typeNames: {})",
                serviceAccountUrn.toString(),
                proposals.size(),
                subTypes.getTypeNames());

            // Batch ingest all proposals
            _entityClient.batchIngestProposals(context.getOperationContext(), proposals, false);

            log.info(
                "User {} created service account {} successfully",
                context.getActorUrn(),
                serviceAccountUrn.toString());

            // Build and return the ServiceAccount response
            return buildServiceAccount(
                serviceAccountUrn.toString(),
                serviceAccountId,
                input.getDisplayName(),
                input.getDescription(),
                context.getActorUrn(),
                System.currentTimeMillis());
          } catch (Exception e) {
            throw new RuntimeException("Failed to create service account", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nonnull
  private MetadataChangeProposal buildProposal(
      Urn entityUrn, String aspectName, RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    proposal.setEntityUrn(entityUrn);
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  @Nonnull
  private ServiceAccount buildServiceAccount(
      String urn,
      String name,
      String displayName,
      String description,
      String createdBy,
      long createdAt) {
    final ServiceAccount serviceAccount = new ServiceAccount();
    serviceAccount.setUrn(urn);
    serviceAccount.setType(EntityType.CORP_USER);
    serviceAccount.setName(name);
    serviceAccount.setDisplayName(displayName);
    serviceAccount.setDescription(description);
    serviceAccount.setCreatedBy(createdBy);
    serviceAccount.setCreatedAt(createdAt);
    return serviceAccount;
  }
}
