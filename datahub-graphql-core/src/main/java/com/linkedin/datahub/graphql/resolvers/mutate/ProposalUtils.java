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
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociationArray;
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
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;


@Slf4j
// TODO(Gabe): Unit test this file
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

    ResourceSpec spec = new ResourceSpec(targetUrn.getEntityType(), targetUrn.toString());
    if (subResource != null && subResource.length() > 0) {
      actors = authorizationManager.authorizedActors(
          PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType(),
          Optional.of(spec)
      );
    } else {
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

  @SneakyThrows
  public static Boolean isTagAlreadyProposedToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityClient entityClient,
      String actor
  ) {
    Filter filter = createActionRequestFilter(
        ActionRequestType.TAG_ASSOCIATION,
        com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING,
        targetUrn.toString(),
        subResource
    );

    return getActionRequestInfosFromFilter(filter, actor, entityClient).filter(actionRequestInfo ->
        (subResource != null ? actionRequestInfo.getSubResource().equals(subResource) : !actionRequestInfo.hasSubResource())
            && actionRequestInfo.getResource().equals(targetUrn.toString())
            && actionRequestInfo.getParams().hasTagProposal()
            && actionRequestInfo.getParams().getTagProposal().getTag().equals(labelUrn)
    ).count() > 0;
  }

  @SneakyThrows
  public static Boolean isTermAlreadyProposedToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityClient entityClient,
      String actor
  ) {
    Filter filter = createActionRequestFilter(
        ActionRequestType.TERM_ASSOCIATION,
        com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING,
        targetUrn.toString(),
        subResource
    );

    return getActionRequestInfosFromFilter(filter, actor, entityClient).filter(actionRequestInfo ->
        (subResource != null ? actionRequestInfo.getSubResource().equals(subResource) : !actionRequestInfo.hasSubResource())
            && actionRequestInfo.getResource().equals(targetUrn.toString())
            && actionRequestInfo.getParams().hasGlossaryTermProposal()
            && actionRequestInfo.getParams().getGlossaryTermProposal().getGlossaryTerm().equals(labelUrn)
    ).count() > 0;
  }

  public static Stream<ActionRequestInfo> getActionRequestInfosFromFilter(
      Filter filter,
      String actor,
      EntityClient entityClient
  ) throws RemoteInvocationException {
    final SearchResult searchResult = entityClient.filter(
        ACTION_REQUEST_ENTITY_NAME,
        filter,
        null,
        0,
        20,
        actor);
    final Map<Urn, Entity> entities = entityClient.batchGet(new HashSet<>(searchResult.getEntities()
        .stream().map(result -> result.getEntity()).collect(Collectors.toList())), actor);

    return entities.values()
        .stream()
        .map(entity -> entity.getValue()
            .getActionRequestSnapshot()
            .getAspects()
            .stream()
            .filter(aspect -> aspect.isActionRequestInfo())
            .map(aspect -> aspect.getActionRequestInfo())
            .findFirst())
        .filter(maybeActionRequestInfo -> maybeActionRequestInfo.isPresent())
        .map(maybeActionRequestInfo -> maybeActionRequestInfo.get());
  }

  public static Boolean isTagAlreadyAttachedToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityService entityService
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlobalTags tags =
          (com.linkedin.common.GlobalTags) getAspectFromEntity(targetUrn.toString(), LabelUtils.TAGS_ASPECT_NAME, entityService, new GlobalTags());

      if (!tags.hasTags()) {
        return false;
      }

      return doesTagsListContainTag(tags, labelUrn);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), LabelUtils.EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);

      if (!editableFieldInfo.hasGlobalTags()) {
        return false;
      }

      return doesTagsListContainTag(editableFieldInfo.getGlobalTags(), labelUrn);
    }
  }

  public static Boolean isTermAlreadyAttachedToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityService entityService
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms) getAspectFromEntity(
              targetUrn.toString(), LabelUtils.GLOSSARY_TERM_ASPECT_NAME, entityService, new GlossaryTerms()
          );

      if (!terms.hasTerms()) {
        return false;
      }

      return doesTermsListContainTerm(terms, labelUrn);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), LabelUtils.EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());

      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        return false;
      }

      return doesTermsListContainTerm(editableFieldInfo.getGlossaryTerms(), labelUrn);
    }
  }

  private static Boolean doesTermsListContainTerm(GlossaryTerms terms, Urn termUrn) {
    if (!terms.hasTerms()) {
      return false;
    }

    GlossaryTermAssociationArray termArray = terms.getTerms();
    return termArray.stream().anyMatch(association -> association.getUrn().equals(termUrn));
  }

  private static Boolean doesTagsListContainTag(GlobalTags tags, Urn tagUrn) {
    if (!tags.hasTags()) {
      return false;
    }

    TagAssociationArray tagAssociationArray = tags.getTags();
    return tagAssociationArray.stream().anyMatch(association -> association.getTag().equals(tagUrn));
  }

  public static Filter createActionRequestFilter(
      final @Nullable ActionRequestType type,
      final @Nullable com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      final @Nonnull String targetUrn,
      final @Nullable String targetSubresource
  ) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();
    if (status != null) {
      andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
    }
    if (type != null) {
      andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
    }
    if (targetSubresource != null) {
      andCriterion.add(ActionRequestUtils.createSubResourceCriterion(targetSubresource));
    }
    andCriterion.add(ActionRequestUtils.createResourceCriterion(targetUrn));

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);

    filter.setOr(disjunction);
    return filter;
  }

  private ProposalUtils() { }
}
