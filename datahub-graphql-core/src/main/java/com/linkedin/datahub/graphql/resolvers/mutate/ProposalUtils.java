package com.linkedin.datahub.graphql.resolvers.mutate;

import com.datahub.metadata.authorization.AuthorizationManager;

import com.datahub.metadata.authorization.ResourceSpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ProposalUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  public static boolean isAuthorizedToProposeTags(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.PROPOSE_DATASET_COL_TAGS_PRIVILEGE.getType()
            : PoliciesConfig.PROPOSE_ENTITY_TAGS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActor(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToProposeTerms(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.PROPOSE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()
            : PoliciesConfig.PROPOSE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActor(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptProposal(
      @Nonnull QueryContext context,
      ActionRequestType type,
      Urn targetUrn,
      String subResource
  ) {
    if (type.equals(ActionRequestType.TAG_ASSOCIATION)) {
      return isAuthorizedToAcceptTags(context, targetUrn, subResource);
    }
    if (type.equals(ActionRequestType.TERM_ASSOCIATION)) {
      return isAuthorizedToAcceptTerms(context, targetUrn, subResource);
    }

    return false;
  }

  public static boolean isAuthorizedToAcceptTerms(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.MANAGE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()
            : PoliciesConfig.MANAGE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActor(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptTags(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType()
            : PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActor(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean proposeTag(
      Urn creator,
      Urn tagUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService entityService,
      AuthorizationManager authorizationManager
  ) {
    AuthorizationManager.AuthorizedActors actors = null;

    if (subResource != null && subResource.length() > 0) {
      ResourceSpec spec = new ResourceSpec(targetUrn.getEntityType(), targetUrn.toString());
      actors = authorizationManager.authorizedActors(
          PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType(),
          Optional.of(spec)
      );
    } else {
      ResourceSpec spec = new ResourceSpec(targetUrn.getEntityType(), targetUrn.toString());
      actors = authorizationManager.authorizedActors(
          PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType(),
          Optional.of(spec)
      );
    }

    List<Urn> assignedUsers = new ArrayList<>();
    List<Urn> assignedGroups = new ArrayList<>();

    if (actors != null) {
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
    }

    ActionRequestSnapshot snapshot = createTagProposalRequest(
        creator,
        assignedUsers,
        assignedGroups,
        tagUrn,
        targetUrn,
        subResource,
        subResourceType
    );

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  private static ActionRequestSnapshot createTagProposalRequest(
      Urn creator,
      List<Urn> assignedUsers,
      List<Urn> assignedGroups,
      Urn tagUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType
  ) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple("actionRequest", uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(creator)));
    aspects.add(ActionRequestAspect.create(createActionRequestInfo(
        ActionRequestType.TAG_ASSOCIATION,
        creator,
        assignedUsers,
        assignedGroups,
        tagUrn,
        targetUrn,
        subResource,
        subResourceType
    )));

    result.setAspects(aspects);

    return result;
  }

  public static boolean proposeTerm(
      Urn creator,
      Urn termUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService entityService,
      AuthorizationManager authorizationManager
  ) {
    AuthorizationManager.AuthorizedActors actors = null;

    if (subResource != null && subResource.length() > 0) {
      ResourceSpec spec = new ResourceSpec(targetUrn.getEntityType(), targetUrn.toString());
      actors = authorizationManager.authorizedActors(
          PoliciesConfig.MANAGE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType(),
          Optional.of(spec)
      );
    } else {
      ResourceSpec spec = new ResourceSpec(targetUrn.getEntityType(), targetUrn.toString());
      actors = authorizationManager.authorizedActors(
          PoliciesConfig.MANAGE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType(),
          Optional.of(spec)
      );
    }

    List<Urn> assignedUsers = new ArrayList<>();
    List<Urn> assignedGroups = new ArrayList<>();

    if (actors != null) {
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
    }

    ActionRequestSnapshot snapshot = createTermProposalRequest(
        creator,
        assignedUsers,
        assignedGroups,
        termUrn,
        targetUrn,
        subResource,
        subResourceType
    );

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  private static ActionRequestSnapshot createTermProposalRequest(
      Urn creator,
      List<Urn> assignedUsers,
      List<Urn> assignedGroups,
      Urn termUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType
  ) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple("actionRequest", uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(creator)));
    aspects.add(ActionRequestAspect.create(createActionRequestInfo(
        ActionRequestType.TERM_ASSOCIATION,
        creator,
        assignedUsers,
        assignedGroups,
        termUrn,
        targetUrn,
        subResource,
        subResourceType
    )));

    result.setAspects(aspects);

    return result;
  }

  public static ActionRequestStatus createActionRequestStatus(Urn creator) {
    final ActionRequestStatus status = new ActionRequestStatus();

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    status.setStatus("PENDING");
    status.setResult("");
    status.setLastModified(auditStamp);

    return status;
  }

  public static ActionRequestInfo createActionRequestInfo(
      ActionRequestType type,
      Urn creator,
      List<Urn> assignedUsers,
      List<Urn> assignedGroups,
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType
  ) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(type.toString());
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setResourceType(targetUrn.getEntityType());
    info.setResource(targetUrn.toString());
    if (subResourceType != null) {
      info.setSubResourceType(subResourceType.toString());
    }
    if (subResource != null) {
      info.setSubResource(subResource);
    }
    info.setParams(createActionRequestParams(labelUrn));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(creator);

    return info;
  }

  public static ActionRequestParams createActionRequestParams(Urn label) {
    ActionRequestParams params = new ActionRequestParams();

    if (label.getEntityType().equals("tag")) {
      TagProposal proposal = new TagProposal();
      proposal.setTag(label);
      params.setTagProposal(proposal);
    }

    if (label.getEntityType().equals("glossaryTerm")) {
      GlossaryTermProposal proposal = new GlossaryTermProposal();
      proposal.setGlossaryTerm(label);
      params.setGlossaryTermProposal(proposal);
    }
    return params;
  }

  public static ActionRequestSnapshot setStatusSnapshot(
      Urn actor,
      com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      ActionRequestResult result,
      Entity proposalEntity
  ) {
    Optional<ActionRequestAspect> actionRequestStatusWrapper =
        proposalEntity.getValue()
            .getActionRequestSnapshot()
            .getAspects()
            .stream()
            .filter(actionRequestAspect -> actionRequestAspect.isActionRequestStatus())
            .findFirst();

    if (!actionRequestStatusWrapper.isPresent()) {
      actionRequestStatusWrapper = Optional.of(ActionRequestAspect.create(ProposalUtils.createActionRequestStatus(actor)));
    }

    ActionRequestStatus statusAspect = actionRequestStatusWrapper.get().getActionRequestStatus();
    statusAspect.setStatus(status.toString());
    statusAspect.setResult(result.toString());

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    statusAspect.setLastModified(auditStamp);

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(statusAspect));

    ActionRequestSnapshot snapshot = new ActionRequestSnapshot();
    snapshot.setUrn(proposalEntity.getValue().getActionRequestSnapshot().getUrn());
    snapshot.setAspects(aspects);

    return snapshot;
  }

  private ProposalUtils() { }
}
