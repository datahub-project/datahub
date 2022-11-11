package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.r2.RemoteInvocationException;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.Optional;

@Slf4j
public class GlossaryUtils {

  private GlossaryUtils() { }

  /**
   * Checks the Platform Privilege MANAGE_GLOSSARIES to see if a user is authorized. If true, the user has global control
   * of their Business Glossary to create, edit, move, and delete Terms and Nodes.
   */
  public static boolean canManageGlossaries(@Nonnull QueryContext context) {
    return AuthorizationUtils.isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE);
  }

  /**
   * Returns true if the current user is able to create, delete, or move Glossary Terms and Nodes under a parent Node.
   * They can do this with either the global MANAGE_GLOSSARIES privilege, or if they have the MANAGE_GLOSSARY_CHILDREN privilege
   * on the relevant parent node in the Glossary.
   */
  public static boolean canManageChildrenEntities(@Nonnull QueryContext context, @Nullable Urn parentNodeUrn) {
    if (canManageGlossaries(context)) {
      return true;
    }
    if (parentNodeUrn == null) {
      return false; // if no parent node, we must rely on the canManageGlossaries method above
    }

    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.MANAGE_GLOSSARY_CHILDREN_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        parentNodeUrn.getEntityType(),
        parentNodeUrn.toString(),
        orPrivilegeGroups);
  }

  /**
   * Returns the urn of the parent node for a given Glossary Term. Returns null if it doesn't exist.
   */
  @Nullable
  private static Urn getTermParentUrn(@Nonnull Urn termUrn, @Nonnull QueryContext context, @Nonnull EntityClient entityClient) {
    try {
      EntityResponse response = entityClient.getV2(Constants.GLOSSARY_TERM_ENTITY_NAME, termUrn,
          ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME), context.getAuthentication());
      if (response != null && response.getAspects().get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME) != null) {
        GlossaryTermInfo termInfo = new GlossaryTermInfo(response.getAspects()
            .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data());
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
  private static Urn getNodeParentUrn(@Nonnull Urn nodeUrn, @Nonnull QueryContext context, @Nonnull EntityClient entityClient) {
    try {
      EntityResponse response = entityClient.getV2(Constants.GLOSSARY_NODE_ENTITY_NAME, nodeUrn,
          ImmutableSet.of(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME), context.getAuthentication());
      if (response != null && response.getAspects().get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME) != null) {
        GlossaryNodeInfo nodeInfo = new GlossaryNodeInfo(response.getAspects()
            .get(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME).getValue().data());
        return nodeInfo.getParentNode();
      }
      return null;
    } catch (URISyntaxException | RemoteInvocationException e) {
      throw new RuntimeException("Failed to fetch Glossary Node to check for privileges", e);
    }
  }

  /**
   * Gets the urn of a Term or Node parent Node. Returns the urn if it exists. Returns null otherwise.
   */
  @Nullable
  public static Urn getParentUrn(@Nonnull Urn urn, @Nonnull QueryContext context, @Nonnull EntityClient entityClient) {
    switch (urn.getEntityType()) {
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return getTermParentUrn(urn, context, entityClient);
      case Constants.GLOSSARY_NODE_ENTITY_NAME:
        return getNodeParentUrn(urn, context, entityClient);
      default:
        log.warn("Tried to get the parent node urn of a non-glossary entity type: {}", urn.getEntityType());
        return null;
    }
  }
}
