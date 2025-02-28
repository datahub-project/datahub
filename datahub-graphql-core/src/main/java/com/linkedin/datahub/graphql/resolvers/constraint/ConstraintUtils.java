package com.linkedin.datahub.graphql.resolvers.constraint;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.constraint.ConstraintInfo;
import com.linkedin.constraint.GlossaryTermInNodeConstraint;
import com.linkedin.container.Container;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Constraint;
import com.linkedin.datahub.graphql.generated.ConstraintParams;
import com.linkedin.datahub.graphql.generated.ConstraintType;
import com.linkedin.datahub.graphql.generated.GlossaryTermInNodeConstraintParams;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConstraintUtils {
  public static final String GLOSSARY_TERMS_ASPECT = "glossaryTerms";

  private ConstraintUtils() {}

  @SneakyThrows
  private static Pair<Boolean, String> isEntityConstraintSatisfied(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull EntitySpec spec,
      @Nonnull ConstraintInfo constraintInfo,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntityClient entityClient) {
    Objects.requireNonNull(urn, "Urn provided to check constraint is null");
    Objects.requireNonNull(constraintInfo, "ConstraintInfo provided to check constraint is null");
    Objects.requireNonNull(entityClient, "entityClient provided to check constraint is null");
    Objects.requireNonNull(opContext, "opContext provided to check constraint is null");

    // By default, the constraint does not apply to this entity - the entity cannot be failing the
    // constraint then
    if (isResourceMatch(constraintInfo.getResources(), spec)) {
      if (constraintInfo.getParams().hasHasGlossaryTermInNodeParams()) {
        return isDatasetOrContainerGlossaryTermConstraintSatisfied(
            opContext, urn, entityService, entityClient, constraintInfo);
      }
    }

    return Pair.of(false, "");
  }

  /** Returns true if the constraint's resource configuration matches the given resource spec */
  public static boolean isResourceMatch(
      final @Nullable DataHubResourceFilter resourceFilter,
      final @Nonnull EntitySpec resourceSpec) {
    if (resourceFilter == null) {
      // No resource filter defined on the constraint. That means the constraint applies to all
      // resources
      return true;
    }

    final boolean resourceTypesMatch =
        resourceFilter.hasType() && resourceFilter.getType().equals(resourceSpec.getType());

    final boolean resourceIdentityMatch =
        resourceFilter.isAllResources()
            || (resourceFilter.hasResources()
                && Objects.requireNonNull(resourceFilter.getResources()).stream()
                    .anyMatch(resource -> resource.equals(resourceSpec.getEntity())));

    // If the resource's type and identity match, then the resource matches the constraint.
    return resourceTypesMatch && resourceIdentityMatch;
  }

  private static Pair<Boolean, String> isDatasetOrContainerGlossaryTermConstraintSatisfied(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull final EntityService entityService,
      @Nonnull final EntityClient entityClient,
      @Nonnull ConstraintInfo constraintInfo)
      throws RemoteInvocationException {

    String glossaryNode =
        constraintInfo
            .getParams()
            .getHasGlossaryTermInNodeParams()
            .getGlossaryNode()
            .getEntityKey()
            .get(0);

    Pair<Boolean, String> datasetConstraintSatisfied =
        isEntityGlossaryTermConstraintSatisfied(opContext, urn, entityClient, constraintInfo);

    if (datasetConstraintSatisfied.getFirst()) {
      return Pair.of(
          true,
          String.format(
              "Constraint requiring %s is satisfied by the dataset Glossary Term %s",
              glossaryNode, datasetConstraintSatisfied.getSecond()));
    }

    Container container =
        (Container)
            EntityUtils.getAspectFromEntity(
                opContext, urn, CONTAINER_ASPECT_NAME, entityService, new Container());
    if (container != null && container.hasContainer()) {
      Urn containerUrn = container.getContainer();
      Pair<Boolean, String> containerConstraintSatisfied =
          isEntityGlossaryTermConstraintSatisfied(
              opContext, containerUrn.toString(), entityClient, constraintInfo);
      if (containerConstraintSatisfied.getFirst()) {
        return Pair.of(
            true,
            String.format(
                "Constraint requiring %s is satisfied by the container Glossary Term %s",
                glossaryNode, containerConstraintSatisfied.getSecond()));
      }
    }

    return Pair.of(false, "");
  }

  private static Pair<Boolean, String> isEntityGlossaryTermConstraintSatisfied(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull final EntityClient entityClient,
      @Nonnull ConstraintInfo constraintInfo)
      throws RemoteInvocationException {
    Optional<GlossaryTerms> glossaryTerms =
        entityClient.getVersionedAspect(
            opContext, urn, GLOSSARY_TERMS_ASPECT, 0L, GlossaryTerms.class);

    return isGlossaryTermConstraintSatisfied(
        opContext,
        constraintInfo.getParams().getHasGlossaryTermInNodeParams(),
        glossaryTerms.orElse(new GlossaryTerms()),
        entityClient);
  }

  /**
   * If the entity passes constraint's glossary term param configuration, returns the glossary term
   * that satisfies the constraint.
   */
  private static Pair<Boolean, String> isGlossaryTermConstraintSatisfied(
      @Nonnull OperationContext opContext,
      final @Nonnull GlossaryTermInNodeConstraint params,
      final @Nonnull GlossaryTerms glossaryTerms,
      final @Nonnull EntityClient entityClient) {
    if (!glossaryTerms.hasTerms()) {
      return Pair.of(false, "");
    }
    Urn glossaryNodeUrn = params.getGlossaryNode();
    return glossaryTerms.getTerms().stream()
        .filter(
            glossaryTermAssociation ->
                isGlossaryNodeInTermsAncestry(
                    opContext, glossaryTermAssociation.getUrn(), glossaryNodeUrn, entityClient))
        .findFirst()
        .map(
            glossaryTermAssociation ->
                Pair.of(true, glossaryTermAssociation.getUrn().getEntityKey().get(0)))
        .orElse(Pair.of(false, ""));
  }

  private static GlossaryTermInfo getGlossaryTermInfo(
      @Nonnull OperationContext opContext,
      final Urn glossaryTermUrn,
      @Nonnull final EntityClient entityClient) {
    try {
      EntityResponse entityResponse =
          entityClient.getV2(
              opContext,
              GLOSSARY_TERM_ENTITY_NAME,
              glossaryTermUrn,
              ImmutableSet.of(GLOSSARY_TERM_INFO_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.hasAspects()
          && entityResponse.getAspects().containsKey(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)) {
        return new GlossaryTermInfo(
            entityResponse
                .getAspects()
                .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
                .getValue()
                .data());
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve Glossary Term Info", e);
    }
  }

  private static GlossaryNodeInfo getGlossaryNodeInfo(
      @Nonnull OperationContext opContext,
      final Urn glossaryNodeUrn,
      @Nonnull final EntityClient entityClient) {
    try {
      EntityResponse entityResponse =
          entityClient.getV2(
              opContext,
              GLOSSARY_NODE_ENTITY_NAME,
              glossaryNodeUrn,
              ImmutableSet.of(GLOSSARY_NODE_INFO_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.hasAspects()
          && entityResponse.getAspects().containsKey(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME)) {
        return new GlossaryNodeInfo(
            entityResponse
                .getAspects()
                .get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME)
                .getValue()
                .data());
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve Glossary Node Info", e);
    }
  }

  private static boolean isGlossaryNodeInTermsAncestry(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn candidateTerm,
      @Nonnull final Urn requiredNode,
      @Nonnull final EntityClient entityClient) {
    GlossaryTermInfo candidateInfo = getGlossaryTermInfo(opContext, candidateTerm, entityClient);
    if (candidateInfo == null) {
      return false;
    }

    GlossaryNodeUrn candidateParentNode = candidateInfo.getParentNode();
    if (candidateParentNode == null) {
      return false;
    }

    if (candidateParentNode.equals(requiredNode)) {
      return true;
    }
    return isGlossaryNodeInNodesAncestry(
        opContext, candidateParentNode, requiredNode, entityClient);
  }

  private static boolean isGlossaryNodeInNodesAncestry(
      @Nonnull OperationContext opContext,
      final @Nonnull Urn candidateNode,
      final @Nonnull Urn requiredParentNode,
      final @Nonnull EntityClient entityClient) {
    GlossaryNodeInfo candidateInfo = getGlossaryNodeInfo(opContext, candidateNode, entityClient);
    if (candidateInfo == null) {
      return false;
    }

    GlossaryNodeUrn candidateParentNode = candidateInfo.getParentNode();

    if (candidateParentNode == null) {
      return false;
    }
    if (candidateParentNode.equals(requiredParentNode)) {
      return true;
    }
    return isGlossaryNodeInNodesAncestry(
        opContext, candidateParentNode, requiredParentNode, entityClient);
  }

  public static Constraint mapConstraintInfoToConstraint(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull EntitySpec spec,
      @Nonnull ConstraintInfo constraintInfo,
      @Nonnull final EntityService entityService,
      @Nonnull final EntityClient entityClient) {
    Constraint constraint = new Constraint();
    constraint.setType(ConstraintType.valueOf(constraintInfo.getType()));
    constraint.setDisplayName(constraintInfo.getDisplayName());
    constraint.setDescription(constraintInfo.getDescription());

    ConstraintParams graphqlParamsContainer = new ConstraintParams();
    if (constraintInfo.getType().equals(ConstraintType.HAS_GLOSSARY_TERM_IN_NODE.toString())) {
      GlossaryTermInNodeConstraintParams graphqlParams = new GlossaryTermInNodeConstraintParams();
      graphqlParams.setNodeName(
          constraintInfo
              .getParams()
              .getHasGlossaryTermInNodeParams()
              .getGlossaryNode()
              .getEntityKey()
              .get(0));
      graphqlParamsContainer.setHasGlossaryTermInNodeParams(graphqlParams);
    }

    constraint.setParams(graphqlParamsContainer);

    Pair<Boolean, String> entityConstraintSatisfied =
        ConstraintUtils.isEntityConstraintSatisfied(
            opContext, urn, spec, constraintInfo, entityService, entityClient);
    constraint.setIsSatisfied(entityConstraintSatisfied.getFirst());
    constraint.setReason(entityConstraintSatisfied.getSecond());

    return constraint;
  }

  public static boolean isAuthorizedToCreateConstraints(final @Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.CREATE_CONSTRAINTS_PRIVILEGE);
  }
}
