package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.metadata.aspect.utils.DeprecationUtils.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeprecationUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private DeprecationUtils() {}

  public static boolean isAuthorizedToUpdateDeprecationForEntity(
      @Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DEPRECATION_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups);
  }

  public static void updateDeprecationForResources(
      @Nonnull OperationContext opContext,
      boolean deprecated,
      @Nullable String note,
      @Nullable Long decommissionTime,
      @Nullable String replacementUrn,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(
          buildUpdateDeprecationProposal(
              opContext,
              deprecated,
              note,
              decommissionTime,
              replacementUrn,
              resource,
              actor,
              entityService));
    }
    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  private static MetadataChangeProposal buildUpdateDeprecationProposal(
      @Nonnull OperationContext opContext,
      boolean deprecated,
      @Nullable String note,
      @Nullable Long decommissionTime,
      @Nullable String replacementUrn,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService) {
    String resourceUrn = resource.getResourceUrn();
    String subResource = resource.getSubResource();
    String targetUrn = "";

    if (subResource == null) {
      targetUrn = resourceUrn;
    } else {
      try {
        targetUrn =
            SchemaFieldUtils.generateSchemaFieldUrn(Urn.createFromString(resourceUrn), subResource)
                .toString();
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    Deprecation deprecation =
        getDeprecation(
            opContext,
            entityService,
            resourceUrn,
            actor,
            note,
            deprecated,
            decommissionTime,
            replacementUrn);

    return MutationUtils.buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(targetUrn), Constants.DEPRECATION_ASPECT_NAME, deprecation);
  }
}
