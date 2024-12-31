package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.authorization.PoliciesConfig.Privilege;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GlossaryUtils {

  private GlossaryUtils() {}

  /**
   * Checks the Platform Privilege MANAGE_GLOSSARIES to see if a user is authorized. If true, the
   * user has global control of their Business Glossary to create, edit, move, and delete Terms and
   * Nodes.
   */
  public static boolean canManageGlossaries(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE);
  }

  // Returns whether this is a glossary entity and whether you can edit this glossary entity with
  // the
  // Manage all children or Manage direct children privileges
  public static boolean canUpdateGlossaryEntity(
      Urn targetUrn, QueryContext context, EntityClient entityClient) {
    final boolean isGlossaryEntity =
        targetUrn.getEntityType().equals(Constants.GLOSSARY_TERM_ENTITY_NAME)
            || targetUrn.getEntityType().equals(Constants.GLOSSARY_NODE_ENTITY_NAME);
    if (!isGlossaryEntity) {
      return false;
    }
    final Urn parentNodeUrn = GlossaryUtils.getParentUrn(targetUrn, context, entityClient);
    return GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn, entityClient);
  }

  /**
   * Returns true if the current user is able to create, delete, or move Glossary Terms and Nodes
   * under a parent Node. They can do this with either the global MANAGE_GLOSSARIES privilege, or if
   * they have the MANAGE_GLOSSARY_CHILDREN privilege on the relevant parent node in the Glossary.
   */
  public static boolean canManageChildrenEntities(
      @Nonnull QueryContext context,
      @Nullable Urn parentNodeUrn,
      @Nonnull EntityClient entityClient) {
    if (canManageGlossaries(context)) {
      return true;
    }
    if (parentNodeUrn == null) {
      return false; // if no parent node, we must rely on the canManageGlossaries method above
    }

    // Check for the MANAGE_GLOSSARY_CHILDREN_PRIVILEGE privilege
    if (hasManagePrivilege(
        context, parentNodeUrn, PoliciesConfig.MANAGE_GLOSSARY_CHILDREN_PRIVILEGE)) {
      return true;
    }

    // Check for the MANAGE_ALL_GLOSSARY_CHILDREN_PRIVILEGE privilege recursively until there is no
    // parent associated.
    Urn currentParentNodeUrn = parentNodeUrn;
    while (currentParentNodeUrn != null) {
      if (hasManagePrivilege(
          context, currentParentNodeUrn, PoliciesConfig.MANAGE_ALL_GLOSSARY_CHILDREN_PRIVILEGE)) {
        return true;
      }
      currentParentNodeUrn = getParentUrn(currentParentNodeUrn, context, entityClient);
    }

    return false;
  }

  public static boolean hasManagePrivilege(
      @Nonnull QueryContext context, @Nullable Urn parentNodeUrn, Privilege privilege) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(new ConjunctivePrivilegeGroup(ImmutableList.of(privilege.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, parentNodeUrn.getEntityType(), parentNodeUrn.toString(), orPrivilegeGroups);
  }

  /**
   * Returns the urn of the parent node for a given Glossary Term. Returns null if it doesn't exist.
   */
  @Nullable
  private static Urn getTermParentUrn(
      @Nonnull Urn termUrn, @Nonnull QueryContext context, @Nonnull EntityClient entityClient) {
    try {
      EntityResponse response =
          entityClient.getV2(
              context.getOperationContext(),
              Constants.GLOSSARY_TERM_ENTITY_NAME,
              termUrn,
              ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME));
      if (response != null
          && response.getAspects().get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME) != null) {
        GlossaryTermInfo termInfo =
            new GlossaryTermInfo(
                response
                    .getAspects()
                    .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
                    .getValue()
                    .data());
        return termInfo.getParentNode();
      }
      return null;
    } catch (URISyntaxException | RemoteInvocationException e) {
      throw new RuntimeException("Failed to fetch Glossary Term to check for privileges", e);
    }
  }

  /**
   * Returns the urn of the parent node for a given Glossary Node. Returns null if it doesn't exist.
   */
  @Nullable
  private static Urn getNodeParentUrn(
      @Nonnull Urn nodeUrn, @Nonnull QueryContext context, @Nonnull EntityClient entityClient) {
    try {
      EntityResponse response =
          entityClient.getV2(
              context.getOperationContext(),
              Constants.GLOSSARY_NODE_ENTITY_NAME,
              nodeUrn,
              ImmutableSet.of(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME));
      if (response != null
          && response.getAspects().get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME) != null) {
        GlossaryNodeInfo nodeInfo =
            new GlossaryNodeInfo(
                response
                    .getAspects()
                    .get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME)
                    .getValue()
                    .data());
        return nodeInfo.getParentNode();
      }
      return null;
    } catch (URISyntaxException | RemoteInvocationException e) {
      throw new RuntimeException("Failed to fetch Glossary Node to check for privileges", e);
    }
  }

  /**
   * Gets the urn of a Term or Node parent Node. Returns the urn if it exists. Returns null
   * otherwise.
   */
  @Nullable
  public static Urn getParentUrn(
      @Nonnull Urn urn, @Nonnull QueryContext context, @Nonnull EntityClient entityClient) {
    switch (urn.getEntityType()) {
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return getTermParentUrn(urn, context, entityClient);
      case Constants.GLOSSARY_NODE_ENTITY_NAME:
        return getNodeParentUrn(urn, context, entityClient);
      default:
        log.warn(
            "Tried to get the parent node urn of a non-glossary entity type: {}",
            urn.getEntityType());
        return null;
    }
  }
}
