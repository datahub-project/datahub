package com.linkedin.datahub.graphql.resolvers.constraint;

import com.datahub.metadata.authorization.ResourceSpec;


import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.constraint.ConstraintInfo;
import com.linkedin.constraint.GlossaryTermInNodeConstraint;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.Constraint;
import com.linkedin.datahub.graphql.generated.ConstraintParams;
import com.linkedin.datahub.graphql.generated.ConstraintType;
import com.linkedin.datahub.graphql.generated.GlossaryTermInNodeConstraintParams;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.r2.RemoteInvocationException;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;


public class ConstraintUtils {
  public static final String GLOSSARY_TERMS_ASPECT = "glossaryTerms";
  public static final String GLOSSARY_TERM_INFO_ASPECT = "glossaryTermInfo";
  public static final String GLOSSARY_NODE_INFO_ASPECT = "glossaryNodeInfo";

  private ConstraintUtils() { }

  private static boolean isEntityFailingGlossaryTermConstraint(
      @Nonnull String urn,
      @Nonnull  final EntityClient entityClient,
      @Nonnull final String actor,
      @Nonnull ConstraintInfo constraintInfo
  ) throws RemoteInvocationException {
      Optional<GlossaryTerms> glossaryTerms =
      entityClient.getVersionedAspect(urn, GLOSSARY_TERMS_ASPECT, 0L, actor, GlossaryTerms.class);

        return !isGlossaryTermConstraintSatisfied(
      constraintInfo.getParams().getHasGlossaryTermInNodeParams(),
            glossaryTerms.orElse(new GlossaryTerms()), entityClient, actor
        );
  }

  @SneakyThrows
  public static boolean isEntityFailingConstraint(
      @Nonnull String urn,
      @Nonnull ResourceSpec spec,
      @Nonnull ConstraintInfo constraintInfo,
      @Nonnull  final EntityClient entityClient,
      @Nonnull final String actor
  ) {
    Objects.requireNonNull(urn, "Urn provided to check constraint is null");
    Objects.requireNonNull(constraintInfo, "ConstraintInfo provided to check constraint is null");
    Objects.requireNonNull(entityClient, "entityClient provided to check constraint is null");
    Objects.requireNonNull(actor, "actor provided to check constraint is null");

    if (isResourceMatch(constraintInfo.getResources(), spec)) {
      if (constraintInfo.getParams().hasHasGlossaryTermInNodeParams()) {
        return isEntityFailingGlossaryTermConstraint(urn, entityClient, actor, constraintInfo);
      }
    }

    // the constraint does not apply to this entity- the entity cannot be failing the constraint then
    return false;
  }

  /**
   * Returns true if the constraint's resource configuration matches the given resource spec
   */
  public static boolean isResourceMatch(
      final @Nullable DataHubResourceFilter resourceFilter,
      final @Nonnull  ResourceSpec resourceSpec
  ) {
    if (resourceFilter == null) {
      // No resource filter defined on the constraint. That means the constraint applies to all resources
      return true;
    }

    final boolean resourceTypesMatch = resourceFilter.hasType() && resourceFilter.getType().equals(resourceSpec.getType());

    final boolean resourceIdentityMatch =
        resourceFilter.isAllResources()
            || (resourceFilter.hasResources() && Objects.requireNonNull(resourceFilter.getResources())
            .stream()
            .anyMatch(resource -> resource.equals(resourceSpec.getResource())));

    // If the resource's type and identity match, then the resource matches the constraint.
    return resourceTypesMatch && resourceIdentityMatch;
  }

  /**
   * Returns true if the entity passses constraint's glossary term param configuration
   */
  private static boolean isGlossaryTermConstraintSatisfied(
      final @Nonnull GlossaryTermInNodeConstraint params,
      final @Nonnull GlossaryTerms glossaryTerms,
      final @Nonnull EntityClient entityClient,
      final @Nonnull String actor
  ) {
    if (!glossaryTerms.hasTerms()) {
      return false;
    }
    Urn glossaryNodeUrn = params.getGlossaryNode();
    return glossaryTerms.getTerms().stream().filter(glossaryTermAssociation -> isGlossaryNodeInTermsAncestry(
        glossaryTermAssociation.getUrn(),
        glossaryNodeUrn,
        entityClient,
        actor
    )).count() > 0;
  }

  private static boolean isGlossaryNodeInTermsAncestry(
      @Nonnull final Urn candidateTerm,
      @Nonnull final Urn requiredNode,
      @Nonnull final EntityClient entityClient,
      @Nonnull final String actor
  ) {
    try {
      Aspect aspect = entityClient.getAspect(candidateTerm.toString(), GLOSSARY_TERM_INFO_ASPECT, 0L, actor).getAspect();
      GlossaryTermInfo candidateInfo = aspect.getGlossaryTermInfo();
      GlossaryNodeUrn candidateParentNode = candidateInfo.getParentNode();
      if (candidateParentNode == null) {
        return false;
      }

      if (candidateParentNode.equals(requiredNode)) {
        return true;
      }
      return isGlossaryNodeInNodesAncestry(
          candidateParentNode,
          requiredNode,
          entityClient,
          actor
      );
    } catch (RemoteInvocationException e) {
      return false;
    }
  }

  private static boolean isGlossaryNodeInNodesAncestry(
      final @Nonnull Urn candidateNode,
      final @Nonnull Urn requiredParentNode,
      final @Nonnull EntityClient entityClient,
      final @Nonnull String actor
  ) {
    try {
      Aspect aspect = entityClient.getAspect(candidateNode.toString(), GLOSSARY_NODE_INFO_ASPECT, 0L, actor).getAspect();
      GlossaryNodeInfo candidateInfo = aspect.getGlossaryNodeInfo();
      GlossaryNodeUrn candidateParentNode = candidateInfo.getParentNode();

      if (candidateParentNode == null) {
        return false;
      }
      if (candidateParentNode.equals(requiredParentNode)) {
        return true;
      }
      return isGlossaryNodeInNodesAncestry(candidateParentNode, requiredParentNode, entityClient, actor);
    } catch (RemoteInvocationException e) {
      return false;
    }
  }

  public static Constraint mapConstraintInfoToConstraint(final @Nonnull ConstraintInfo constraintInfo) {
    Constraint constraint = new Constraint();
    constraint.setType(ConstraintType.valueOf(constraintInfo.getType()));
    constraint.setDisplayName(constraintInfo.getDisplayName());
    constraint.setDescription(constraintInfo.getDescription());

    ConstraintParams graphqlParamsContainer = new ConstraintParams();
    if (constraintInfo.getType().equals(ConstraintType.HAS_GLOSSARY_TERM_IN_NODE.toString())) {
      GlossaryTermInNodeConstraintParams graphqlParams = new GlossaryTermInNodeConstraintParams();
      graphqlParams.setNodeName(
          constraintInfo.getParams().getHasGlossaryTermInNodeParams().getGlossaryNode().getEntityKey().get(0)
      );
      graphqlParamsContainer.setHasGlossaryTermInNodeParams(graphqlParams);
    }

    constraint.setParams(graphqlParamsContainer);
    return constraint;
  }

  public static boolean isAuthorizedToCreateConstraints(final @Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        new ConjunctivePrivilegeGroup(ImmutableList.of(
            PoliciesConfig.CREATE_CONSTRAINTS_PRIVILEGE.getType()
        ))
    ));

    return AuthorizationUtils.isAuthorized(context.getAuthorizer(), context.getActor(), orPrivilegeGroups);
  }
}
