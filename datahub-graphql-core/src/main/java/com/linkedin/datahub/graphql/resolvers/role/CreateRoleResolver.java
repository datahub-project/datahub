package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateRoleInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubRoleKey;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver for creating a new DataHub Role */
@Slf4j
@RequiredArgsConstructor
public class CreateRoleResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateRoleInput input =
        bindArgument(environment.getArgument("input"), CreateRoleInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Check if the user has permission to create roles
            if (!AuthorizationUtils.canManagePolicies(context)) {
              throw new AuthorizationException(
                  "Unauthorized to create DataHub Roles. Please contact your DataHub administrator.");
            }

            // Validate input
            if (input.getName() == null || input.getName().trim().isEmpty()) {
              throw new IllegalArgumentException("Role name cannot be empty");
            }

            // Generate a unique key for the new role
            final String roleId = UUID.randomUUID().toString();
            final DataHubRoleKey key = new DataHubRoleKey();
            key.setId(roleId);

            // Create the DataHubRoleInfo aspect
            final DataHubRoleInfo roleInfo =
                new DataHubRoleInfo()
                    .setName(input.getName().trim())
                    .setDescription(
                        input.getDescription() != null ? input.getDescription().trim() : "")
                    .setEditable(true); // Custom roles are editable by default

            // Build the metadata change proposal
            final com.linkedin.mxe.MetadataChangeProposal proposal =
                MutationUtils.buildMetadataChangeProposalWithKey(
                    key,
                    Constants.DATAHUB_ROLE_ENTITY_NAME,
                    Constants.DATAHUB_ROLE_INFO_ASPECT_NAME,
                    roleInfo);

            // Ingest the role
            String urn =
                _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

            // Associate the role with policies if specified
            log.info(
                "CreateRole: Processing policy associations. PolicyUrns: {}",
                input.getPolicyUrns());
            if (input.getPolicyUrns() != null && !input.getPolicyUrns().isEmpty()) {
              log.info(
                  "CreateRole: Associating role {} with {} policies",
                  urn,
                  input.getPolicyUrns().size());
              associateRoleWithPolicies(context, urn, input.getPolicyUrns());
            } else {
              log.info("CreateRole: No policy associations provided");
            }

            log.info("Successfully created DataHub Role with URN: {}", urn);
            return urn;

          } catch (Exception e) {
            log.error("Failed to create DataHub Role", e);
            throw new RuntimeException("Failed to create DataHub Role: " + e.getMessage(), e);
          }
        });
  }

  /**
   * Associates a role with the specified policies by adding the role to each policy's actors.roles
   * field
   */
  private void associateRoleWithPolicies(
      QueryContext context, String roleUrn, List<String> policyUrns) throws Exception {
    final Urn roleUrnObj = UrnUtils.getUrn(roleUrn);
    log.info("Associating role {} with policies: {}", roleUrn, policyUrns);

    for (String policyUrnStr : policyUrns) {
      try {
        final Urn policyUrn = UrnUtils.getUrn(policyUrnStr);

        // Fetch the existing policy
        final EntityResponse policyResponse =
            _entityClient.getV2(
                context.getOperationContext(), Constants.POLICY_ENTITY_NAME, policyUrn, null);

        if (policyResponse == null) {
          log.warn("Policy not found: {}, skipping role association", policyUrnStr);
          continue;
        }

        // Get the existing policy info
        final DataHubPolicyInfo existingPolicyInfo =
            new DataHubPolicyInfo(
                policyResponse
                    .getAspects()
                    .get(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME)
                    .getValue()
                    .data());

        // Create a copy of the policy info to modify
        final DataHubPolicyInfo updatedPolicyInfo = existingPolicyInfo.copy();

        // Get or create the actors filter
        DataHubActorFilter actorFilter = updatedPolicyInfo.getActors();
        if (actorFilter == null) {
          actorFilter = new DataHubActorFilter();
          updatedPolicyInfo.setActors(actorFilter);
        }

        // Add the role to the actors.roles list
        UrnArray existingRoles = actorFilter.getRoles();
        if (existingRoles == null) {
          existingRoles = new UrnArray();
        } else {
          existingRoles = new UrnArray(existingRoles); // Create a mutable copy
        }

        // Add the role if it's not already present
        if (!existingRoles.contains(roleUrnObj)) {
          existingRoles.add(roleUrnObj);
          actorFilter.setRoles(existingRoles);

          // Build the metadata change proposal
          final com.linkedin.mxe.MetadataChangeProposal policyProposal =
              MutationUtils.buildMetadataChangeProposalWithUrn(
                  policyUrn, Constants.DATAHUB_POLICY_INFO_ASPECT_NAME, updatedPolicyInfo);

          // Ingest the updated policy
          log.info(
              "About to ingest updated policy {} with role {} added to actors",
              policyUrnStr,
              roleUrn);
          String policyIngestionResult =
              _entityClient.ingestProposal(context.getOperationContext(), policyProposal, false);
          log.info(
              "Successfully associated role {} with policy {}. Ingestion result: {}",
              roleUrn,
              policyUrnStr,
              policyIngestionResult);

          // Verify the relationship was created by trying to read it back
          try {
            final EntityResponse verifyResponse =
                _entityClient.getV2(
                    context.getOperationContext(), Constants.POLICY_ENTITY_NAME, policyUrn, null);
            if (verifyResponse != null) {
              final DataHubPolicyInfo verifyPolicyInfo =
                  new DataHubPolicyInfo(
                      verifyResponse
                          .getAspects()
                          .get(Constants.DATAHUB_POLICY_INFO_ASPECT_NAME)
                          .getValue()
                          .data());
              DataHubActorFilter verifyActorFilter = verifyPolicyInfo.getActors();
              if (verifyActorFilter != null && verifyActorFilter.getRoles() != null) {
                log.info(
                    "Verification: Policy {} now has {} roles: {}",
                    policyUrnStr,
                    verifyActorFilter.getRoles().size(),
                    verifyActorFilter.getRoles());
              } else {
                log.warn("Verification: Policy {} has no roles after ingestion", policyUrnStr);
              }
            }
          } catch (Exception verifyError) {
            log.warn("Failed to verify policy association: {}", verifyError.getMessage());
          }
        }

      } catch (Exception e) {
        log.error(
            "Failed to associate role {} with policy {}: {}",
            roleUrn,
            policyUrnStr,
            e.getMessage(),
            e);
        // Continue with other policies instead of failing completely
      }
    }
  }
}
