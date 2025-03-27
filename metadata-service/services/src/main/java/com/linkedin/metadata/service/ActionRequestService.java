package com.linkedin.metadata.service;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL;
import static com.linkedin.metadata.Constants.ACTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ACTION_REQUEST_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ACTION_REQUEST_STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_SCHEMA_PROPOSALS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_CONTRACT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_CONTRACT_STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EDITABLE_DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ENTITY_PROPOSALS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_NODE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.EntitySpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestOrigin;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.CreateGlossaryNodeProposal;
import com.linkedin.actionrequest.CreateGlossaryTermProposal;
import com.linkedin.actionrequest.DataContractProposal;
import com.linkedin.actionrequest.DataContractProposalOperationType;
import com.linkedin.actionrequest.DescriptionProposal;
import com.linkedin.actionrequest.DomainProposal;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.OwnerProposal;
import com.linkedin.actionrequest.StructuredPropertyProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.ai.InferenceMetadata;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.Proposals;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.SetMode;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataContractState;
import com.linkedin.datacontract.DataContractStatus;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.DataQualityContractArray;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.FreshnessContractArray;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datacontract.SchemaContractArray;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.GlossaryNodeKey;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaProposal;
import com.linkedin.schema.SchemaProposalArray;
import com.linkedin.schema.SchemaProposals;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ActionRequestService extends BaseService {
  static final String STATUS_FIELD_NAME = "status";
  static final String TYPE_FIELD_NAME = "type";
  static final String RESULT_FIELD_NAME = "result";
  static final String RESOURCE_FIELD_NAME = "resource";
  static final String SUBRESOURCE_FIELD_NAME = "subResource";
  static final String LAST_MODIFIED_FIELD_NAME = "lastModified";
  static final String TAG_ASSOCIATION_PROPOSAL_TYPE = "TAG_ASSOCIATION";
  static final String TERM_ASSOCIATION_PROPOSAL_TYPE = "TERM_ASSOCIATION";
  static final String DOMAIN_ASSOCIATION_PROPOSAL_TYPE = "DOMAIN_ASSOCIATION";
  static final String OWNER_ASSOCIATION_PROPOSAL_TYPE = "OWNER_ASSOCIATION";
  static final String STRUCTURED_PROPERTY_ASSOCIATION_PROPOSAL_TYPE =
      "STRUCTURED_PROPERTY_ASSOCIATION";
  static final String CREATE_GLOSSARY_NODE_ACTION_REQUEST_TYPE = "CREATE_GLOSSARY_NODE";
  private static final String CREATE_GLOSSARY_TERM_ACTION_REQUEST_TYPE = "CREATE_GLOSSARY_TERM";
  private static final String UPDATE_DESCRIPTION_ACTION_REQUEST_TYPE = "UPDATE_DESCRIPTION";
  private static final String DATA_CONTRACT_REQUEST_TYPE = "DATA_CONTRACT";
  static final String ACTION_REQUEST_STATUS_PENDING = "PENDING";
  static final String ACTION_REQUEST_STATUS_COMPLETE = "COMPLETE";
  static final String ACTION_REQUEST_RESULT_ACCEPTED = "ACCEPTED";
  private static final String ACTION_REQUEST_EMPTY_RESULT = "";

  private final EntityService<?> entityService;
  private final GraphClient graphClient;
  private final DatasetService datasetService;
  private final TagService tagService;
  private final OwnerService ownerService;
  private final GlossaryTermService glossaryTermService;
  private final StructuredPropertyService structuredPropertyService;
  private final DomainService domainService;

  public ActionRequestService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull EntityService<?> entityService,
      @Nonnull GraphClient graphClient,
      @Nonnull DatasetService datasetService,
      @Nonnull TagService tagService,
      @Nonnull GlossaryTermService glossaryTermService,
      @Nonnull StructuredPropertyService structuredPropertyService,
      @Nonnull DomainService domainService,
      @Nonnull OwnerService ownerService,
      @Nonnull OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
    this.entityService = entityService;
    this.graphClient = graphClient;
    this.datasetService = datasetService;
    this.tagService = tagService;
    this.glossaryTermService = glossaryTermService;
    this.structuredPropertyService = structuredPropertyService;
    this.domainService = domainService;
    this.ownerService = ownerService;
  }

  /** General get assignee method */
  @Nullable
  public List<String> getAssignee(
      @Nonnull final OperationContext opContext,
      @Nonnull final String actionType,
      @Nonnull final Urn resourceUrn,
      @Nullable final String subResourceUrn,
      @Nullable final SubResourceType subResourceType) {

    List<String> result = null;
    AuthorizedActors actors = null;
    AssignedActors assignedActors = null;
    switch (actionType) {
      case DATA_CONTRACT_REQUEST_TYPE:
        actors = getContractAssociationAssignee(opContext, resourceUrn);
        break;
      case OWNER_ASSOCIATION_PROPOSAL_TYPE:
        actors = getOwnersAssociationAssignees(opContext, resourceUrn);
        break;
      case DOMAIN_ASSOCIATION_PROPOSAL_TYPE:
        actors = getDomainAssociationAssignees(opContext, resourceUrn);
        break;
      case TAG_ASSOCIATION_PROPOSAL_TYPE:
        actors = getTagAssociationAssignees(opContext, resourceUrn, subResourceType);
        break;
      case TERM_ASSOCIATION_PROPOSAL_TYPE:
        actors = getTermAssociationAssignees(opContext, resourceUrn, subResourceType);
        break;
      case STRUCTURED_PROPERTY_ASSOCIATION_PROPOSAL_TYPE:
        actors = getStructuredPropertyAssociationAssignees(opContext, resourceUrn, subResourceType);
        break;
      default:
        switch (resourceUrn.getEntityType()) {
          case GLOSSARY_NODE_ENTITY_NAME:
          case GLOSSARY_TERM_ENTITY_NAME:
            assignedActors =
                getAssignedUsersAndGroupsForGlossary(
                    opContext,
                    PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(),
                    Optional.of(resourceUrn));
          default:
            log.error("Not implemented for actionType={}, entityType={}", actionType, resourceUrn);
            break;
        }
    }

    if (actors != null) {
      List<Urn> tmp = actors.getUsers();
      tmp.addAll(actors.getGroups());
      tmp.addAll(actors.getRoles());
      result = tmp.stream().map(Urn::toString).collect(Collectors.toList());
    } else if (assignedActors != null) {
      List<Urn> tmp = assignedActors.getUsers();
      tmp.addAll(assignedActors.getGroups());
      tmp.addAll(assignedActors.getRoles());
      result = tmp.stream().map(Urn::toString).collect(Collectors.toList());
    } else {
      result = Collections.emptyList();
    }
    return result;
  }

  /*--------------------------------------------------------------------------
   *                   TAG PROPOSAL METHODS
   *------------------------------------------------------------------------*/
  /**
   * Propose tags for a given asset. Note that authorization must be checked before calling this
   * method.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the asset to apply tags to
   * @param tagUrns the urns of the tags to apply
   * @param description the description for proposal
   * @return the urn of the action request
   */
  public Urn proposeEntityTags(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> tagUrns,
      @Nullable final String description)
      throws AlreadyRequestedException,
          AlreadyAppliedException,
          RemoteInvocationException,
          URISyntaxException {

    // Step 1: Validate inputs
    validateProposeAssetTags(opContext, entityUrn, tagUrns);

    // Step 2: Filter out any tags that should not be proposed (either already applied, or already
    // attached).
    final List<Urn> finalTagUrns = filterEntityTagsToPropose(opContext, entityUrn, tagUrns);

    if (finalTagUrns.isEmpty()) {
      throw new AlreadyAppliedException("All tags are already applied or proposed for the entity");
    }

    // Step 3: Propose the tag change.
    return createTagAssociationActionRequest(
        opContext, entityUrn, finalTagUrns, null, null, description);
  }

  /**
   * Propose schema field tags for a given entity and field path. Note that authorization must be
   * checked before calling this method.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the entity to apply tags to
   * @param schemaFieldPath the field path to apply tags to
   * @param tagUrns the urns of the tags to apply
   * @param description the description for proposal
   * @return the urn of the action request
   */
  public Urn proposeSchemaFieldTags(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath,
      @Nonnull final List<Urn> tagUrns,
      @Nullable final String description)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          MalformedActionRequestException,
          RemoteInvocationException,
          URISyntaxException {

    // Step 1: Validate inputs
    validateProposeSchemaFieldTags(opContext, entityUrn, schemaFieldPath, tagUrns);

    // Step 2: Filter Tags to Propose
    final List<Urn> finalTagUrns =
        filterSchemaFieldTagsToPropose(opContext, entityUrn, tagUrns, schemaFieldPath);

    if (finalTagUrns.isEmpty()) {
      throw new AlreadyAppliedException("All tags are already applied or proposed for the entity");
    }

    // Step 2: Propose the tag change.
    return createTagAssociationActionRequest(
        opContext,
        entityUrn,
        finalTagUrns,
        SubResourceType.DATASET_FIELD,
        schemaFieldPath,
        description);
  }

  /*--------------------------------------------------------------------------
   *                   TERM PROPOSAL METHODS
   *------------------------------------------------------------------------*/
  /**
   * Propose glossary terms for a given asset. Note that authorization must be checked before
   * calling this.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the asset to apply terms to
   * @param termUrns the urns of the glossary terms to apply
   * @param description the description for proposal
   * @return the urn of the action request
   */
  public Urn proposeEntityTerms(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> termUrns,
      @Nullable final String description)
      throws AlreadyRequestedException,
          AlreadyAppliedException,
          RemoteInvocationException,
          URISyntaxException {

    // Step 1: Validate inputs
    validateProposeAssetTerms(opContext, entityUrn, termUrns);

    // Step 2: Filter out any terms that should not be proposed
    final List<Urn> finalTermUrns = filterEntityTermsToPropose(opContext, entityUrn, termUrns);

    if (finalTermUrns.isEmpty()) {
      throw new AlreadyAppliedException("All terms are already applied or proposed for the entity");
    }

    // Step 3: Propose the term change
    return createTermAssociationActionRequest(
        opContext, entityUrn, finalTermUrns, null, null, description);
  }

  /**
   * Propose glossary terms for a given entity's schema field path.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the entity to apply terms to
   * @param schemaFieldPath the field path
   * @param termUrns the urns of the glossary terms to apply
   * @param description the description for proposal
   * @return the urn of the action request
   */
  public Urn proposeSchemaFieldTerms(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath,
      @Nonnull final List<Urn> termUrns,
      @Nullable final String description)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          MalformedActionRequestException,
          RemoteInvocationException,
          URISyntaxException {

    // Step 1: Validate inputs
    validateProposeSchemaFieldTerms(opContext, entityUrn, schemaFieldPath, termUrns);

    // Step 2: Filter out any terms that should not be proposed
    final List<Urn> finalTermUrns =
        filterSchemaFieldTermsToPropose(opContext, entityUrn, termUrns, schemaFieldPath);

    if (finalTermUrns.isEmpty()) {
      throw new AlreadyAppliedException(
          "All terms are already applied or proposed for the schema field");
    }

    // Step 3: Propose the term change
    return createTermAssociationActionRequest(
        opContext,
        entityUrn,
        finalTermUrns,
        SubResourceType.DATASET_FIELD,
        schemaFieldPath,
        description);
  }

  /*--------------------------------------------------------------------------
   *                   STRUCTURED PROPERTY PROPOSAL METHODS
   *------------------------------------------------------------------------*/
  /**
   * Propose structured properties for a given asset. Note that authorization must be checked before
   * calling this.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the asset to apply properties to
   * @param propertyValueAssignments the new property value assignments
   * @param description the description for proposal
   * @return the urn of the action request
   */
  public Urn proposeEntityStructuredProperties(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<StructuredPropertyValueAssignment> propertyValueAssignments,
      @Nullable final String description)
      throws AlreadyRequestedException,
          AlreadyAppliedException,
          RemoteInvocationException,
          URISyntaxException,
          MalformedActionRequestException {

    // Step 1: Validate inputs
    validateProposeAssetStructuredProperties(opContext, entityUrn, propertyValueAssignments);

    // Step 2: Filter out any property that should not be proposed
    final List<StructuredPropertyValueAssignment> finalPropertyAssignments =
        filterEntityStructuredPropertiesToPropose(opContext, entityUrn, propertyValueAssignments);

    if (finalPropertyAssignments.isEmpty()) {
      throw new AlreadyAppliedException(
          "All structured properties are already applied or proposed for the entity");
    }

    // Step 3: Propose the structured property change
    return createStructuredPropertyAssociationActionRequest(
        opContext, entityUrn, finalPropertyAssignments, null, null, description);
  }

  /**
   * Propose structured properties for a given entity's schema field path.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the entity to apply properties to
   * @param schemaFieldPath the field path
   * @param propertyValueAssignments the urns of the structured properties to apply
   * @param description the description for proposal
   * @return the urn of the action request
   */
  public Urn proposeSchemaFieldStructuredProperties(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath,
      @Nonnull final List<StructuredPropertyValueAssignment> propertyValueAssignments,
      @Nullable final String description)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          MalformedActionRequestException,
          RemoteInvocationException,
          URISyntaxException {

    // Step 1: Validate inputs
    validateProposeSchemaFieldStructuredProperties(
        opContext, entityUrn, schemaFieldPath, propertyValueAssignments);

    // Step 2: Filter out any properties that should not be proposed
    final List<StructuredPropertyValueAssignment> finalPropertyAssignments =
        filterSchemaFieldStructuredPropertiesToPropose(
            opContext, entityUrn, propertyValueAssignments, schemaFieldPath);

    if (finalPropertyAssignments.isEmpty()) {
      throw new AlreadyAppliedException(
          "All structured properties are already applied or proposed for the schema field");
    }

    // Step 3: Propose the structured properties change
    return createStructuredPropertyAssociationActionRequest(
        opContext,
        entityUrn,
        finalPropertyAssignments,
        SubResourceType.DATASET_FIELD,
        schemaFieldPath,
        description);
  }

  /*--------------------------------------------------------------------------
   *                   DOMAIN PROPOSAL METHODS
   *------------------------------------------------------------------------*/
  /**
   * Propose domain for a given asset. Note that authorization must be checked before calling this.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the asset to apply terms to
   * @param domainUrn the urns of the domain to apply
   * @param description the description for proposal
   * @return the urn of the action request
   */
  public Urn proposeEntityDomain(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn domainUrn,
      @Nullable final String description)
      throws AlreadyRequestedException,
          AlreadyAppliedException,
          RemoteInvocationException,
          URISyntaxException {

    // Step 1: Validate inputs
    validateProposeAssetDomain(opContext, entityUrn, domainUrn);

    // Step 2: Verify domains that have already been applied and proposed
    final Urn finalDomainUrn = filterEntityDomainToPropose(opContext, entityUrn, domainUrn);

    // Step 3: Propose the domain change
    return createDomainAssociationActionRequest(
        opContext, entityUrn, finalDomainUrn, null, null, description);
  }

  /*--------------------------------------------------------------------------
   *                   OWNERS PROPOSAL METHODS
   *------------------------------------------------------------------------*/
  /**
   * Propose owners for a given asset. Note that authorization must be checked before calling this.
   *
   * @param opContext the operation context
   * @param entityUrn the urn of the asset to apply terms to
   * @param owners the owners to propose
   * @param description the description for proposal
   * @return the urn of the action request
   */
  public Urn proposeEntityOwners(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Owner> owners,
      @Nullable final String description)
      throws AlreadyRequestedException,
          AlreadyAppliedException,
          RemoteInvocationException,
          URISyntaxException {

    // Step 1: Validate inputs
    validateProposeAssetOwners(opContext, entityUrn, owners);

    // Step 2: Filter out any owners that should not be proposed
    final List<Owner> finalOwners = filterEntityOwnersToPropose(opContext, entityUrn, owners);

    if (finalOwners.isEmpty()) {
      throw new AlreadyAppliedException(
          "All owners are already applied or proposed for the entity");
    }

    // Step 3: Propose the owners change
    return createOwnersAssociationActionRequest(opContext, entityUrn, finalOwners, description);
  }

  /*--------------------------------------------------------------------------
   *            ACCEPT STRUCTURED PROPERTY PROPOSAL
   *------------------------------------------------------------------------*/

  /**
   * Accept a structured property proposal by urn. Assumes that the authorization has ALREADY been
   * validated outside (e.g. in a resolver).
   *
   * @param opContext the operation context
   * @param actionRequestUrn the action request urn
   * @throws Exception if the proposal cannot be accepted
   */
  public void acceptStructuredPropertyProposal(
      @Nonnull OperationContext opContext, @Nonnull final Urn actionRequestUrn)
      throws MalformedActionRequestException, RemoteInvocationException {
    Objects.requireNonNull(actionRequestUrn, "actionRequestUrn cannot be null");
    final ActionRequestInfo maybeInfo = getActionRequestInfo(opContext, actionRequestUrn);
    if (maybeInfo == null) {
      throw new EntityDoesNotExistException(
          String.format("Action request with urn %s does not exist.", actionRequestUrn));
    }
    acceptStructuredPropertyProposal(opContext, maybeInfo);
  }

  /**
   * Accept a structured property proposal. Assumes that the authorization has ALREADY been
   * validated outside (e.g. in a resolver).
   *
   * @param opContext the operation context
   * @param actionRequestInfo the action request info
   */
  public void acceptStructuredPropertyProposal(
      @Nonnull final OperationContext opContext, @Nonnull final ActionRequestInfo actionRequestInfo)
      throws RemoteInvocationException, MalformedActionRequestException {
    Objects.requireNonNull(actionRequestInfo, "actionRequestInfo cannot be null");

    if (!ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL.equals(actionRequestInfo.getType())) {
      throw new MalformedActionRequestException(
          "Action request is not a structured property proposal");
    }

    if (!actionRequestInfo.hasParams()
        || !actionRequestInfo.getParams().hasStructuredPropertyProposal()) {
      throw new MalformedActionRequestException(
          "Action request does not contain structured property proposal");
    }

    final StructuredPropertyProposal structuredPropertyProposal =
        actionRequestInfo.getParams().getStructuredPropertyProposal();
    final List<StructuredPropertyValueAssignment> propertyValueAssignments =
        structuredPropertyProposal.getStructuredPropertyValues();

    final Urn entityUrn = UrnUtils.getUrn(actionRequestInfo.getResource());
    final String maybeSchemaField = actionRequestInfo.getSubResource();

    if (maybeSchemaField != null) {
      // Case 1: Update properties for schema field.
      Urn schemaFieldUrn =
          UrnUtils.getUrn(String.format("urn:li:schemaField:(%s,%s)", entityUrn, maybeSchemaField));
      this.structuredPropertyService.updateEntityStructuredProperties(
          opContext, schemaFieldUrn, propertyValueAssignments);
    } else {
      // Case 2: Update properties for entity.
      this.structuredPropertyService.updateEntityStructuredProperties(
          opContext, entityUrn, propertyValueAssignments);
    }
  }

  /*--------------------------------------------------------------------------
   *            PROPOSE CREATION OF GLOSSARY NODE
   *------------------------------------------------------------------------*/
  public boolean proposeCreateGlossaryNode(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description,
      @Nullable final String proposalNote) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(name, "name cannot be null");
    Objects.requireNonNull(parentNode, "parentNode cannot be null");

    AssignedActors actors =
        getAssignedUsersAndGroupsForGlossary(
            opContext, PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(), parentNode);
    List<Urn> assignedUsers = actors.getUsers();
    List<Urn> assignedGroups = actors.getGroups();
    List<Urn> assignedRoles = actors.getRoles();
    ActionRequestSnapshot snapshot =
        createCreateGlossaryNodeProposalActionRequest(
            actorUrn,
            assignedUsers,
            assignedGroups,
            assignedRoles,
            name,
            parentNode,
            description,
            proposalNote);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    this.entityService.ingestEntity(opContext, entity, auditStamp);

    return true;
  }

  /*--------------------------------------------------------------------------
   *             AUTHORIZE REVIEW OF GLOSSARY NODE PROPOSAL
   *------------------------------------------------------------------------*/
  public boolean canResolveGlossaryNodeProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final boolean canManageGlossaries) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    if (canManageGlossaries) {
      return true;
    }

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    try {
      CreateGlossaryNodeProposal createGlossaryNodeProposal =
          actionRequestInfo.getParams().getCreateGlossaryNodeProposal();
      Optional<Urn> parentNode =
          createGlossaryNodeProposal.hasParentNode()
                  && createGlossaryNodeProposal.getParentNode() != null
              ? Optional.of(createGlossaryNodeProposal.getParentNode())
              : Optional.empty();
      return isAuthorizedToResolveGlossaryEntityAsOwner(opContext, actorUrn, parentNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create glossary term entity");
    }
  }

  /*--------------------------------------------------------------------------
   *             ACCEPT OR REJECT GLOSSARY NODE PROPOSAL
   *------------------------------------------------------------------------*/
  public void acceptCreateGlossaryNodeProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final boolean canManageGlossaries)
      throws Exception {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    CreateGlossaryNodeProposal createGlossaryNodeProposal =
        actionRequestInfo.getParams().getCreateGlossaryNodeProposal();
    String name = createGlossaryNodeProposal.getName();
    Optional<Urn> parentNode =
        createGlossaryNodeProposal.hasParentNode()
                && createGlossaryNodeProposal.getParentNode() != null
            ? Optional.of(createGlossaryNodeProposal.getParentNode())
            : Optional.empty();
    Optional<String> description =
        createGlossaryNodeProposal.hasDescription()
            ? Optional.ofNullable(createGlossaryNodeProposal.getDescription())
            : Optional.empty();
    if (!canManageGlossaries
        && !isAuthorizedToResolveGlossaryEntityAsOwner(opContext, actorUrn, parentNode)) {
      throw new RuntimeException(
          "Unauthorized to accept creating this Glossary Node. Please contact your DataHub administrator.");
    }

    createGlossaryNodeEntity(opContext, name, parentNode, description);
  }

  /*--------------------------------------------------------------------------
   *             PROPOSE CREATION OF GLOSSARY TERM
   *------------------------------------------------------------------------*/
  public boolean proposeCreateGlossaryTerm(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description,
      @Nullable final String proposalNote) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(name, "name cannot be null");
    Objects.requireNonNull(parentNode, "parentNode cannot be null");

    AssignedActors actors =
        getAssignedUsersAndGroupsForGlossary(
            opContext, PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(), parentNode);
    List<Urn> assignedUsers = actors.getUsers();
    List<Urn> assignedGroups = actors.getGroups();
    List<Urn> assignedRoles = actors.getRoles();

    ActionRequestSnapshot snapshot =
        createCreateGlossaryTermProposalActionRequest(
            actorUrn,
            assignedUsers,
            assignedGroups,
            assignedRoles,
            name,
            parentNode,
            description,
            proposalNote);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    this.entityService.ingestEntity(opContext, entity, auditStamp);

    return true;
  }

  /*--------------------------------------------------------------------------
   *             AUTHORIZE REVIEW OF GLOSSARY TERM PROPOSAL
   *------------------------------------------------------------------------*/
  public boolean canResolveGlossaryTermProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final boolean canManageGlossaries) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    if (canManageGlossaries) {
      return true;
    }

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    try {
      CreateGlossaryTermProposal createGlossaryTermProposal =
          actionRequestInfo.getParams().getCreateGlossaryTermProposal();
      Optional<Urn> parentNode =
          createGlossaryTermProposal.hasParentNode()
                  && createGlossaryTermProposal.getParentNode() != null
              ? Optional.of(createGlossaryTermProposal.getParentNode())
              : Optional.empty();
      return isAuthorizedToResolveGlossaryEntityAsOwner(opContext, actorUrn, parentNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create glossary term entity");
    }
  }

  /*--------------------------------------------------------------------------
   *             ACCEPT OR REJECT GLOSSARY TERM PROPOSAL
   *------------------------------------------------------------------------*/
  public void acceptCreateGlossaryTermProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final boolean canManageGlossaries)
      throws Exception {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    CreateGlossaryTermProposal createGlossaryTermProposal =
        actionRequestInfo.getParams().getCreateGlossaryTermProposal();
    String name = createGlossaryTermProposal.getName();
    Optional<Urn> parentNode =
        createGlossaryTermProposal.hasParentNode()
                && createGlossaryTermProposal.getParentNode() != null
            ? Optional.of(createGlossaryTermProposal.getParentNode())
            : Optional.empty();
    Optional<String> description =
        createGlossaryTermProposal.hasDescription()
            ? Optional.ofNullable(createGlossaryTermProposal.getDescription())
            : Optional.empty();
    if (!canManageGlossaries
        && !isAuthorizedToResolveGlossaryEntityAsOwner(opContext, actorUrn, parentNode)) {
      throw new RuntimeException(
          "Unauthorized to accept creating this Glossary Node. Please contact your DataHub administrator.");
    }

    createGlossaryTermEntity(opContext, name, parentNode, description);
  }

  /*--------------------------------------------------------------------------
   *             PROPOSE UPDATING DESCRIPTIONS
   *------------------------------------------------------------------------*/
  public boolean proposeUpdateResourceDescription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn resourceUrn,
      final String subResourceType,
      final String subResource,
      @Nonnull final String description,
      @Nullable final String proposalNote)
      throws RemoteInvocationException {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(resourceUrn, "resourceUrn cannot be null");
    Objects.requireNonNull(description, "description cannot be null");

    if (!this.entityClient.exists(opContext, resourceUrn, true)) {
      throw new RuntimeException(String.format("Entity %s does not exist", resourceUrn));
    }

    List<Urn> assignedUsers;
    List<Urn> assignedGroups;
    List<Urn> assignedRoles;
    EntitySpec spec = new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString());

    if (resourceUrn.getEntityType().equals(GLOSSARY_TERM_ENTITY_NAME)
        || resourceUrn.getEntityType().equals(GLOSSARY_NODE_ENTITY_NAME)) {
      AssignedActors actors =
          getAssignedUsersAndGroupsForGlossary(
              opContext,
              PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(),
              Optional.of(resourceUrn));
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
      assignedRoles = actors.getRoles();
    } else {
      AuthorizedActors actors =
          opContext
              .getAuthorizationContext()
              .getAuthorizer()
              .authorizedActors(
                  PoliciesConfig.MANAGE_ENTITY_DOCS_PROPOSALS_PRIVILEGE.getType(),
                  Optional.of(spec));
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
      assignedRoles = actors.getRoles();
    }

    // TODO found a bug but not fixing right now
    //  with the if condition of GLOSSARY_TERM_ENTITY_NAME and GLOSSARY_NODE_ENTITY_NAME this is
    // wrong
    // and needs to be fixed. Action type cannot be UPDATE_DESCRIPTION_ACTION_REQUEST_TYPE
    ActionRequestSnapshot snapshot =
        createUpdateDescriptionProposalActionRequest(
            actorUrn,
            resourceUrn,
            subResourceType,
            subResource,
            assignedUsers,
            assignedGroups,
            assignedRoles,
            description,
            proposalNote);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    this.entityService.ingestEntity(opContext, entity, auditStamp);

    return true;
  }

  /*--------------------------------------------------------------------------
   *             ACCEPT OR REJECT DESCRIPTION PROPOSAL
   *------------------------------------------------------------------------*/
  public void acceptUpdateResourceDescriptionProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot)
      throws Exception {
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    if (!actionRequestInfo.hasResource()) {
      throw new RuntimeException("actionRequestInfo is missing resource");
    }
    Urn resourceUrn = Urn.createFromString(actionRequestInfo.getResource());
    DescriptionProposal descriptionProposal =
        actionRequestInfo.getParams().getUpdateDescriptionProposal();
    String description = descriptionProposal.getDescription();
    switch (resourceUrn.getEntityType()) {
      case GLOSSARY_NODE_ENTITY_NAME:
        updateGlossaryNodeDescription(opContext, resourceUrn, description);
        break;
      case GLOSSARY_TERM_ENTITY_NAME:
        updateGlossaryTermDescription(opContext, resourceUrn, description);
        break;
      case DATASET_ENTITY_NAME:
        String subResourceType = actionRequestInfo.getSubResourceType(GetMode.NULL);
        if (!Strings.isNullOrEmpty(subResourceType)
            && SubResourceType.valueOf(subResourceType) == SubResourceType.DATASET_FIELD) {
          updateSchemaFieldDescription(
              opContext,
              resourceUrn,
              Objects.requireNonNull(actionRequestInfo.getSubResource()),
              description);
        } else {
          updateDatasetDescription(opContext, resourceUrn, description);
        }
        break;
      default:
        log.warn(
            String.format(
                "Proposing an update to a description is currently not supported for entity type %s",
                resourceUrn.getEntityType()));
        break;
    }
  }

  /*--------------------------------------------------------------------------
   *             PROPOSE CREATION OF DATA CONTRACT (BETA)
   *------------------------------------------------------------------------*/
  public boolean proposeDataContract(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final DataContractProposalOperationType opType,
      @Nullable final List<FreshnessContract> freshness,
      @Nullable final List<SchemaContract> schema,
      @Nullable final List<DataQualityContract> quality,
      @Nullable final String description)
      throws RemoteInvocationException {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(entityUrn, "entityUrn cannot be null");
    Objects.requireNonNull(opType, "opType cannot be null");

    if (!this.entityClient.exists(opContext, entityUrn, true)) {
      throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
    }

    if (freshness != null) {
      verifyAssertionsExist(
          opContext,
          freshness.stream().map(FreshnessContract::getAssertion).collect(Collectors.toList()));
    }

    if (schema != null) {
      verifyAssertionsExist(
          opContext,
          schema.stream().map(SchemaContract::getAssertion).collect(Collectors.toList()));
    }

    if (quality != null) {
      verifyAssertionsExist(
          opContext,
          quality.stream().map(DataQualityContract::getAssertion).collect(Collectors.toList()));
    }

    List<Urn> assignedUsers;
    List<Urn> assignedGroups;

    AuthorizedActors actors = getContractAssociationAssignee(opContext, entityUrn);
    assignedUsers = actors.getUsers();
    assignedGroups = actors.getGroups();

    ActionRequestSnapshot snapshot =
        createDataContractActionRequest(
            actorUrn,
            assignedUsers,
            assignedGroups,
            entityUrn,
            opType,
            freshness,
            schema,
            quality,
            description);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    this.entityService.ingestEntity(opContext, entity, auditStamp);

    return true;
  }

  private AuthorizedActors getContractAssociationAssignee(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    EntitySpec spec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());
    return opContext
        .getAuthorizationContext()
        .getAuthorizer()
        .authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_DATA_CONTRACT_PROPOSALS_PRIVILEGE.getType(),
            Optional.of(spec));
  }

  /*--------------------------------------------------------------------------
   *             ACCEPT OR REJECT DATA CONTRACT PROPOSAL (BETA)
   *------------------------------------------------------------------------*/
  public void acceptDataContractProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot)
      throws Exception {
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");
    Objects.requireNonNull(opContext, "opContext cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    DataContractProposal dataContractProposal =
        actionRequestInfo.getParams().getDataContractProposal();
    if (dataContractProposal != null) {
      final Urn entityUrn = UrnUtils.getUrn(actionRequestInfo.getResource());
      final Urn existingContractUrn = getDataContractUrn(opContext, entityUrn);
      final DataContractProperties existingProperties =
          existingContractUrn != null
              ? getDataContractProperties(opContext, existingContractUrn)
              : null;
      final Urn finalContractUrn =
          existingContractUrn != null
              ? existingContractUrn
              : Urn.createFromString(String.format("urn:li:dataContract:%s", UUID.randomUUID()));
      final DataContractProperties newProperties =
          new DataContractProperties()
              .setEntity(entityUrn)
              .setFreshness(dataContractProposal.getFreshness(GetMode.NULL), SetMode.IGNORE_NULL)
              .setDataQuality(
                  dataContractProposal.getDataQuality(GetMode.NULL), SetMode.IGNORE_NULL)
              .setSchema(dataContractProposal.getSchema(GetMode.NULL), SetMode.IGNORE_NULL);
      final DataContractProperties finalProperties =
          existingProperties != null
              ? mergeDataContractProperties(
                  dataContractProposal.getType(), existingProperties, newProperties)
              : newProperties;

      // TODO: If the contract contains external assertions, then mark it as pending.
      final DataContractStatus status =
          new DataContractStatus().setState(getStateForContract(entityUrn, dataContractProposal));

      final MetadataChangeProposal propertiesProposal = new MetadataChangeProposal();
      propertiesProposal.setEntityUrn(finalContractUrn);
      propertiesProposal.setEntityType(DATA_CONTRACT_ENTITY_NAME);
      propertiesProposal.setAspectName(DATA_CONTRACT_PROPERTIES_ASPECT_NAME);
      propertiesProposal.setAspect(GenericRecordUtils.serializeAspect(finalProperties));
      propertiesProposal.setChangeType(ChangeType.UPSERT);

      final MetadataChangeProposal statusProposal = new MetadataChangeProposal();
      statusProposal.setEntityUrn(finalContractUrn);
      statusProposal.setEntityType(DATA_CONTRACT_ENTITY_NAME);
      statusProposal.setAspectName(DATA_CONTRACT_STATUS_ASPECT_NAME);
      statusProposal.setAspect(GenericRecordUtils.serializeAspect(status));
      statusProposal.setChangeType(ChangeType.UPSERT);

      // TODO: In the future, we may need to mint Monitors to execute the assertions if they
      // are executable.
      // Currently, we are expecting that the assertions will be purely external.
      this.entityClient.batchIngestProposals(
          opContext, ImmutableList.of(propertiesProposal, statusProposal));
    } else {
      throw new IllegalArgumentException(
          "Failed to accept Data Contract Proposal. Action Request is missing required parameters.");
    }
  }

  /*--------------------------------------------------------------------------
   *             MARK A PROPOSAL AS ACCEPTED OR REJECTED
   *------------------------------------------------------------------------*/
  /**
   * Mark a proposal as completed. TODO: Migrate to calling this internally, instead of requiring an
   * external caller.
   *
   * @param opContext the operation context
   * @param actorUrn the actor who is completing the proposal
   * @param actionRequestStatus the status of the action request to set
   * @param actionRequestResult the result of the action request to set
   * @param note an optional note to attach to the resolved status
   * @param proposalEntity the entity representing the proposal
   */
  public void completeProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final String actionRequestStatus,
      @Nonnull final String actionRequestResult,
      @Nullable final String note,
      @Nonnull final Entity proposalEntity) {

    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestStatus, "actionRequestStatus cannot be null");
    Objects.requireNonNull(actionRequestResult, "actionRequestResult cannot be null");

    final ActionRequestSnapshot actionRequestSnapshot =
        setStatusSnapshot(actorUrn, actionRequestStatus, actionRequestResult, note, proposalEntity);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(actionRequestSnapshot));
    this.entityService.ingestEntity(opContext, entity, auditStamp);
  }

  /*--------------------------------------------------------------------------
   *             RETRIEVE ACTION REQUEST DETAILS
   *------------------------------------------------------------------------*/
  @Nullable
  public ActionRequestInfo getActionRequestInfo(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getActionRequestInfoEntityResponse(opContext, entityUrn);
    if (response != null && response.getAspects().containsKey(ACTION_REQUEST_INFO_ASPECT_NAME)) {
      return new ActionRequestInfo(
          response.getAspects().get(ACTION_REQUEST_INFO_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  @Nullable
  private EntityResponse getActionRequestInfoEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(ACTION_REQUEST_INFO_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve action request info for entity with urn %s", entityUrn),
          e);
    }
  }

  /*--------------------------------------------------------------------------
   *                   VALIDATION METHODS FOR TERMS
   *------------------------------------------------------------------------*/
  private void validateProposeAssetTerms(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> termUrns)
      throws EntityDoesNotExistException, RemoteInvocationException {

    // Step 1: Check if the entity exists
    if (!this.entityClient.exists(opContext, entityUrn, false)) {
      throw new EntityDoesNotExistException(String.format("Entity %s does not exist", entityUrn));
    }

    // Step 2: Check if the terms exist
    for (Urn termUrn : termUrns) {
      if (!this.entityClient.exists(opContext, termUrn, false)) {
        throw new EntityDoesNotExistException(
            String.format("Term with urn %s does not exist", termUrn));
      }
    }
  }

  private void validateProposeSchemaFieldTerms(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath,
      @Nonnull final List<Urn> termUrns)
      throws EntityDoesNotExistException,
          MalformedActionRequestException,
          RemoteInvocationException {

    // Step 1: Check if the entity exists
    if (!this.entityClient.exists(opContext, entityUrn, false)) {
      throw new EntityDoesNotExistException(String.format("Entity %s does not exist", entityUrn));
    }

    // Step 2: Check if the field path exists
    if (!this.datasetService.schemaFieldExists(opContext, entityUrn, schemaFieldPath)) {
      throw new EntityDoesNotExistException(
          String.format("Field path %s does not exist in entity %s", schemaFieldPath, entityUrn));
    }

    // Step 3: Check if the terms exist
    for (Urn termUrn : termUrns) {
      if (!this.entityClient.exists(opContext, termUrn, false)) {
        throw new EntityDoesNotExistException(
            String.format("Term with urn %s does not exist", termUrn));
      }
    }

    // Step 4: Ensure that the entity urn is a dataset urn when proposing for a schema field
    if (!entityUrn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
      throw new MalformedActionRequestException("Entity urn must be a dataset urn");
    }
  }

  /*--------------------------------------------------------------------------
   *                     FILTER METHODS FOR TERMS
   *------------------------------------------------------------------------*/

  private List<Urn> filterEntityTermsToPropose(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> termUrns)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          RemoteInvocationException,
          URISyntaxException {

    // Get the currently-applied Terms
    final List<GlossaryTermAssociation> appliedTerms =
        this.glossaryTermService.getEntityTerms(opContext, entityUrn);
    // Get the Terms already proposed
    final List<Urn> proposedTerms = getProposedTermsForEntity(opContext, entityUrn);

    // Deep check whether all are already applied
    if (appliedTerms.size() == termUrns.size()
        && appliedTerms.stream()
            .allMatch(
                applied ->
                    termUrns.stream()
                        .anyMatch(t -> t.toString().equals(applied.getUrn().toString())))) {
      throw new AlreadyAppliedException("All terms are already applied to the entity");
    }

    // Deep check whether all are already proposed
    if (proposedTerms.size() == termUrns.size()
        && proposedTerms.stream()
            .allMatch(
                proposed ->
                    termUrns.stream().anyMatch(t -> t.toString().equals(proposed.toString())))) {
      throw new AlreadyRequestedException("All terms are already proposed for the entity");
    }

    // Filter out Terms that are either already applied or already proposed
    return termUrns.stream()
        .filter(
            termUrn ->
                appliedTerms.stream()
                    .noneMatch(
                        termAssociation ->
                            termAssociation.getUrn().toString().equals(termUrn.toString())))
        .filter(
            termUrn ->
                proposedTerms.stream()
                    .noneMatch(proposedTerm -> proposedTerm.toString().equals(termUrn.toString())))
        .collect(Collectors.toList());
  }

  private List<Urn> filterSchemaFieldTermsToPropose(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> termUrns,
      @Nonnull final String schemaFieldPath)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          RemoteInvocationException,
          URISyntaxException {

    // Get the currently-applied Terms on the field
    final List<GlossaryTermAssociation> appliedTerms =
        this.glossaryTermService.getSchemaFieldTerms(opContext, entityUrn, schemaFieldPath);
    // Get the Terms already proposed
    final List<Urn> proposedTerms =
        getProposedTermsForSchemaField(opContext, entityUrn, schemaFieldPath);

    // Deep check whether all are already applied
    if (appliedTerms.size() == termUrns.size()
        && appliedTerms.stream()
            .allMatch(
                applied ->
                    termUrns.stream()
                        .anyMatch(t -> t.toString().equals(applied.getUrn().toString())))) {
      throw new AlreadyAppliedException("All terms are already applied to the schema field");
    }

    // Deep check whether all are already proposed
    if (proposedTerms.size() == termUrns.size()
        && proposedTerms.stream()
            .allMatch(
                proposed ->
                    termUrns.stream().anyMatch(t -> t.toString().equals(proposed.toString())))) {
      throw new AlreadyRequestedException("All terms are already proposed for the schema field");
    }

    // Filter out Terms that are either already applied or already proposed
    return termUrns.stream()
        .filter(
            termUrn ->
                appliedTerms.stream()
                    .noneMatch(
                        termAssociation ->
                            termAssociation.getUrn().toString().equals(termUrn.toString())))
        .filter(
            termUrn ->
                proposedTerms.stream()
                    .noneMatch(proposedTerm -> proposedTerm.toString().equals(termUrn.toString())))
        .collect(Collectors.toList());
  }

  /*--------------------------------------------------------------------------
   *             CREATION OF ACTION REQUESTS FOR TERMS
   *------------------------------------------------------------------------*/
  private Urn createTermAssociationActionRequest(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> termUrns,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nullable final String description)
      throws RemoteInvocationException {

    // First, get the assignees (authorized actors) for the term change proposal.
    final AuthorizedActors assignees =
        getTermAssociationAssignees(opContext, entityUrn, subResourceType);

    // Then, create the action request
    final List<MetadataChangeProposal> mcps =
        createTermAssociationActionRequestMcps(
            entityUrn,
            termUrns,
            subResourceType,
            subResource,
            assignees.getUsers(),
            assignees.getGroups(),
            assignees.getRoles(),
            opContext.getAuditStamp().getActor(),
            description);

    // Ingest the aspects
    this.entityClient.batchIngestProposals(opContext, mcps, false);

    // Finally, update the secondary indexing aspect
    addTermsToProposalsAspect(opContext, entityUrn, subResourceType, subResource, termUrns);

    // Return the URN of the new action request.
    return mcps.get(0).getEntityUrn();
  }

  @Nonnull
  private List<MetadataChangeProposal> createTermAssociationActionRequestMcps(
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> termUrns,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final Urn actorUrn,
      @Nullable final String description) {

    return createActionRequestMcps(
        TERM_ASSOCIATION_PROPOSAL_TYPE,
        entityUrn,
        subResourceType,
        subResource,
        createGlossaryTermAssociationActionRequestParams(termUrns),
        assignedUsers,
        assignedGroups,
        assignedRoles,
        actorUrn,
        description);
  }

  @Nonnull
  private ActionRequestParams createGlossaryTermAssociationActionRequestParams(
      @Nonnull final List<Urn> termUrns) {
    final ActionRequestParams params = new ActionRequestParams();
    GlossaryTermProposal termProposal = new GlossaryTermProposal();
    termProposal.setGlossaryTerms(new UrnArray(termUrns));
    params.setGlossaryTermProposal(termProposal);
    return params;
  }

  private void addTermsToProposalsAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nonnull final List<Urn> termUrns)
      throws RemoteInvocationException {

    if (SubResourceType.DATASET_FIELD.equals(subResourceType)) {
      addTermToSchemaProposalsAspect(opContext, entityUrn, subResource, termUrns);
    } else {
      addTermToEntityProposalsAspect(opContext, entityUrn, termUrns);
    }
  }

  private void addTermToSchemaProposalsAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath,
      @Nonnull final List<Urn> termUrns)
      throws RemoteInvocationException {
    final SchemaProposals schemaProposals =
        getSchemaFieldProposalsOrDefault(opContext, entityUrn, new SchemaProposals());

    if (!schemaProposals.hasSchemaProposals()) {
      schemaProposals.setSchemaProposals(new SchemaProposalArray());
    }

    final SchemaProposal updatedSchemaFieldProposal =
        schemaProposals.getSchemaProposals().stream()
            .filter(s -> s.getFieldPath().equals(schemaFieldPath))
            .findFirst()
            .orElse(new SchemaProposal().setFieldPath(schemaFieldPath));

    // Suppose we store "ProposedSchemaTerms" on the SchemaProposal
    final Set<Urn> updatedSchemaFieldTermUrns =
        new HashSet<>(
            updatedSchemaFieldProposal.hasProposedSchemaGlossaryTerms()
                ? updatedSchemaFieldProposal.getProposedSchemaGlossaryTerms()
                : Collections.emptyList());

    // Add the new terms
    updatedSchemaFieldTermUrns.addAll(termUrns);
    updatedSchemaFieldProposal.setProposedSchemaGlossaryTerms(
        new UrnArray(updatedSchemaFieldTermUrns));

    // Filter out old proposals for that field, add the new one
    final List<SchemaProposal> finalSchemaProposals =
        schemaProposals.getSchemaProposals().stream()
            .filter(proposal -> !schemaFieldPath.equals(proposal.getFieldPath()))
            .collect(Collectors.toList());
    finalSchemaProposals.add(updatedSchemaFieldProposal);
    schemaProposals.setSchemaProposals(new SchemaProposalArray(finalSchemaProposals));

    final MetadataChangeProposal schemaProposalsMcp =
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, Constants.DATASET_SCHEMA_PROPOSALS_ASPECT_NAME, schemaProposals);

    this.entityClient.ingestProposal(opContext, schemaProposalsMcp, false);
  }

  private void addTermToEntityProposalsAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> termUrns)
      throws RemoteInvocationException {

    final Proposals entityProposals =
        getEntityProposalsOrDefault(opContext, entityUrn, new Proposals());

    if (!entityProposals.hasProposedGlossaryTerms()) {
      entityProposals.setProposedGlossaryTerms(new UrnArray());
    }

    final Set<Urn> updatedTermUrns = new HashSet<>(entityProposals.getProposedGlossaryTerms());
    updatedTermUrns.addAll(termUrns);

    entityProposals.setProposedGlossaryTerms(new UrnArray(updatedTermUrns));

    final MetadataChangeProposal proposal =
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, ENTITY_PROPOSALS_ASPECT_NAME, entityProposals);

    this.entityClient.ingestProposal(opContext, proposal, false);
  }

  /*--------------------------------------------------------------------------
   *             GET PROPOSED TERMS HELPER METHODS
   *------------------------------------------------------------------------*/

  private List<Urn> getProposedTermsForEntity(
      @Nonnull final OperationContext opContext, @Nonnull final Urn entityUrn)
      throws RemoteInvocationException, URISyntaxException {

    final Filter filter =
        createActionRequestFilter(
            TERM_ASSOCIATION_PROPOSAL_TYPE,
            ACTION_REQUEST_STATUS_PENDING,
            entityUrn.toString(),
            null,
            null);

    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
        .flatMap(
            actionRequestInfo ->
                actionRequestInfo.getParams().getGlossaryTermProposal().hasGlossaryTerms()
                    ? actionRequestInfo
                        .getParams()
                        .getGlossaryTermProposal()
                        .getGlossaryTerms()
                        .stream()
                    : Stream.empty())
        .collect(Collectors.toList());
  }

  private List<Urn> getProposedTermsForSchemaField(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath)
      throws RemoteInvocationException, URISyntaxException {

    final Filter filter =
        createActionRequestFilter(
            TERM_ASSOCIATION_PROPOSAL_TYPE,
            ACTION_REQUEST_STATUS_PENDING,
            entityUrn.toString(),
            schemaFieldPath,
            null);

    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
        .filter(actionRequestInfo -> schemaFieldPath.equals(actionRequestInfo.getSubResource()))
        .flatMap(
            actionRequestInfo ->
                actionRequestInfo.getParams().getGlossaryTermProposal().hasGlossaryTerms()
                    ? actionRequestInfo
                        .getParams()
                        .getGlossaryTermProposal()
                        .getGlossaryTerms()
                        .stream()
                    : Stream.empty())
        .collect(Collectors.toList());
  }

  @Nonnull
  private AuthorizedActors getTermAssociationAssignees(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nullable final SubResourceType subResourceType) {

    EntitySpec spec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());
    if (SubResourceType.DATASET_FIELD.equals(subResourceType)) {
      return opContext
          .getAuthorizationContext()
          .getAuthorizer()
          .authorizedActors(
              PoliciesConfig.MANAGE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType(), // hypothetical
              Optional.of(spec));
    }
    // Otherwise, handle top-level entity Terms
    return opContext
        .getAuthorizationContext()
        .getAuthorizer()
        .authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType(), // hypothetical
            Optional.of(spec));
  }

  /*--------------------------------------------------------------------------
   *                   VALIDATION METHODS FOR TAGS
   *------------------------------------------------------------------------*/
  private void validateProposeAssetTags(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> tagUrns)
      throws RemoteInvocationException {
    // Step 1: Check if the entity exists
    if (!this.entityClient.exists(opContext, entityUrn, false)) {
      throw new EntityDoesNotExistException(String.format("Entity %s does not exist", entityUrn));
    }

    // Step 2: Check if the tags exist
    for (Urn tagUrn : tagUrns) {
      if (!this.entityClient.exists(opContext, tagUrn, false)) {
        throw new EntityDoesNotExistException(
            String.format("Tag with urn %s does not exist", tagUrn));
      }
    }
  }

  private void validateProposeSchemaFieldTags(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath,
      @Nonnull final List<Urn> tagUrns)
      throws MalformedActionRequestException, RemoteInvocationException {
    // Step 1: Check if the entity exists
    if (!this.entityClient.exists(opContext, entityUrn, false)) {
      throw new EntityDoesNotExistException(String.format("Entity %s does not exist", entityUrn));
    }

    // Step 2: Check if the field path exists in the entity
    if (!this.datasetService.schemaFieldExists(opContext, entityUrn, schemaFieldPath)) {
      throw new EntityDoesNotExistException(
          String.format("Field path %s does not exist in entity %s", schemaFieldPath, entityUrn));
    }

    // Step 3: Check if the tags exist
    for (Urn tagUrn : tagUrns) {
      if (!this.entityClient.exists(opContext, tagUrn, false)) {
        throw new EntityDoesNotExistException(
            String.format("Tag with urn %s does not exist", tagUrn));
      }
    }

    // Step 6: Ensure that the entity urn is a dataset urn when proposing for a schema field
    if (!entityUrn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
      throw new MalformedActionRequestException("Entity urn must be a dataset urn");
    }
  }

  /*--------------------------------------------------------------------------
   *                     FILTER METHODS FOR TAGS
   *------------------------------------------------------------------------*/
  private List<Urn> filterEntityTagsToPropose(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> tagUrns)
      throws RemoteInvocationException,
          URISyntaxException,
          AlreadyAppliedException,
          AlreadyRequestedException {
    final List<TagAssociation> tags = this.tagService.getEntityTags(opContext, entityUrn);
    final List<Urn> proposedTags = getProposedTagsForEntity(opContext, entityUrn);

    // Deep check whether all tags are already applied
    if (tags.size() == tagUrns.size()
        && tags.stream()
            .allMatch(
                tag ->
                    tagUrns.stream()
                        .anyMatch(tagUrn -> tag.getTag().toString().equals(tagUrn.toString())))) {
      throw new AlreadyAppliedException("All tags are already applied to the entity");
    }

    // Deep check whether all tags are already proposed
    if (proposedTags.size() == tagUrns.size()
        && proposedTags.stream()
            .allMatch(
                tagUrn ->
                    tagUrns.stream()
                        .anyMatch(
                            proposedTag -> proposedTag.toString().equals(tagUrn.toString())))) {
      throw new AlreadyRequestedException("All tags are already proposed for the entity");
    }

    return tagUrns.stream()
        .filter(
            tagUrn ->
                tags.stream().noneMatch(tag -> tag.getTag().toString().equals(tagUrn.toString())))
        .filter(
            tagUrn ->
                proposedTags.stream()
                    .noneMatch(proposedTag -> proposedTag.toString().equals(tagUrn.toString())))
        .collect(Collectors.toList());
  }

  private List<Urn> filterSchemaFieldTagsToPropose(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> tagUrns,
      @Nonnull final String schemaFieldPath)
      throws RemoteInvocationException,
          URISyntaxException,
          AlreadyAppliedException,
          AlreadyRequestedException {
    final List<TagAssociation> tags =
        this.tagService.getSchemaFieldTags(opContext, entityUrn, schemaFieldPath);
    final List<Urn> proposedTags =
        getProposedTagsForSchemaField(opContext, entityUrn, schemaFieldPath);

    // Deep check whether all tags are already applied
    if (tags.size() == tagUrns.size()
        && tags.stream()
            .allMatch(
                tag ->
                    tagUrns.stream()
                        .anyMatch(tagUrn -> tag.getTag().toString().equals(tagUrn.toString())))) {
      throw new AlreadyAppliedException("All tags are already applied to the schema field");
    }
    // Deep check whether all tags are already proposed
    if (proposedTags.size() == tagUrns.size()
        && proposedTags.stream()
            .allMatch(
                tagUrn ->
                    tagUrns.stream()
                        .anyMatch(
                            proposedTag -> proposedTag.toString().equals(tagUrn.toString())))) {
      throw new AlreadyRequestedException("All tags are already proposed for the schema field");
    }

    return tagUrns.stream()
        .filter(
            tagUrn ->
                tags.stream().noneMatch(tag -> tag.getTag().toString().equals(tagUrn.toString())))
        .filter(
            tagUrn ->
                proposedTags.stream()
                    .noneMatch(proposedTag -> proposedTag.toString().equals(tagUrn.toString())))
        .collect(Collectors.toList());
  }

  /*--------------------------------------------------------------------------
   *             CREATION OF ACTION REQUESTS FOR TAGS
   *------------------------------------------------------------------------*/
  private Urn createTagAssociationActionRequest(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> tagUrns,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nullable final String description)
      throws RemoteInvocationException {

    // First, get the assignees for the tag change proposal.
    final AuthorizedActors assignees =
        getTagAssociationAssignees(opContext, entityUrn, subResourceType);

    // Then, create the action request
    final List<MetadataChangeProposal> mcps =
        createTagAssociationActionRequestMcps(
            entityUrn,
            tagUrns,
            subResourceType,
            subResource,
            assignees.getUsers(),
            assignees.getGroups(),
            assignees.getRoles(),
            opContext.getAuditStamp().getActor(),
            description);

    // Finally, Ingest the aspects!
    this.entityClient.batchIngestProposals(opContext, mcps, false);

    // At the very end, at to the secondary indexing aspect!
    addTagsToProposalsAspect(opContext, entityUrn, subResourceType, subResource, tagUrns);

    // Return the URN of the new action request.
    return mcps.get(0).getEntityUrn();
  }

  @Nonnull
  private List<MetadataChangeProposal> createTagAssociationActionRequestMcps(
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> tagUrns,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final Urn actorUrn,
      @Nullable final String description) {
    return createActionRequestMcps(
        TAG_ASSOCIATION_PROPOSAL_TYPE,
        entityUrn,
        subResourceType,
        subResource,
        createTagAssociationActionRequestParams(tagUrns),
        assignedUsers,
        assignedGroups,
        assignedRoles,
        actorUrn,
        description);
  }

  @Nonnull
  private List<MetadataChangeProposal> createActionRequestMcps(
      @Nonnull final String actionRequestType,
      @Nonnull final Urn entityUrn,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nonnull final ActionRequestParams params,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final Urn actorUrn,
      @Nullable final String description) {
    final Urn actionRequestUrn =
        UrnUtils.getUrn(String.format("urn:li:actionRequest:%s", UUID.randomUUID()));

    // 1. Build Action Request Info
    final ActionRequestInfo actionRequestInfo =
        createActionRequestInfo(
            actionRequestType,
            entityUrn,
            subResourceType,
            subResource,
            params,
            ActionRequestOrigin.MANUAL,
            null, // TODO: Should we not allow our AI systems to use this API? Why would it produce
            // aspects directly?
            assignedUsers,
            assignedGroups,
            assignedRoles,
            actorUrn,
            description);
    final MetadataChangeProposal actionRequestInfoProposal =
        AspectUtils.buildMetadataChangeProposal(
            actionRequestUrn, ACTION_REQUEST_INFO_ASPECT_NAME, actionRequestInfo);

    // 2. Build Action Request Status
    final ActionRequestStatus actionRequestStatus = createPendingActionRequestStatus(actorUrn);
    final MetadataChangeProposal actionRequestStatusProposal =
        AspectUtils.buildMetadataChangeProposal(
            actionRequestUrn, ACTION_REQUEST_STATUS_ASPECT_NAME, actionRequestStatus);

    return ImmutableList.of(actionRequestInfoProposal, actionRequestStatusProposal);
  }

  @Nonnull
  private AuthorizedActors getTagAssociationAssignees(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nullable final SubResourceType subResourceType) {
    EntitySpec spec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());

    // Case 1: Handle Column Tag Association
    if (SubResourceType.DATASET_FIELD.equals(subResourceType)) {
      return opContext
          .getAuthorizationContext()
          .getAuthorizer()
          .authorizedActors(
              PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType(), Optional.of(spec));
    }
    // Case 2: Handle non-column Tag Associations.
    return opContext
        .getAuthorizationContext()
        .getAuthorizer()
        .authorizedActors(PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType(), Optional.of(spec));
  }

  // Adds to a secondary aspect for quickly lookup. TBH, I thought this was done in a hook. Not bad
  // to see it done here,
  // but it means anyone who is producing directly may have some problems.
  private void addTagsToProposalsAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nonnull final List<Urn> tagUrns)
      throws RemoteInvocationException {
    if (SubResourceType.DATASET_FIELD.equals(subResourceType)) {
      addTagToSchemaProposalsAspect(opContext, entityUrn, subResource, tagUrns);
    } else {
      addTagToEntityProposalsAspect(opContext, entityUrn, tagUrns);
    }
  }

  // Note that this is NOT safe, I just don't have time to migrate the legacy code I ported
  // to be PATCH semantics.
  // This also just feels super messy. It seems like this should be in a hook or MCP mutator.
  private void addTagToSchemaProposalsAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath,
      @Nonnull final List<Urn> tagUrns)
      throws RemoteInvocationException {
    final SchemaProposals schemaProposals =
        getSchemaFieldProposalsOrDefault(opContext, entityUrn, new SchemaProposals());

    if (!schemaProposals.hasSchemaProposals()) {
      schemaProposals.setSchemaProposals(new SchemaProposalArray());
    }

    final SchemaProposal updatedSchemaFieldProposal =
        schemaProposals.getSchemaProposals().stream()
            .filter(s -> s.getFieldPath().equals(schemaFieldPath))
            .findFirst()
            .orElse(new SchemaProposal().setFieldPath(schemaFieldPath));

    final Set<Urn> updatedSchemaFieldTagUrns =
        new HashSet<>(
            updatedSchemaFieldProposal.hasProposedSchemaTags()
                ? updatedSchemaFieldProposal.getProposedSchemaTags()
                : Collections.emptyList());

    // Add the new tags to existing ones.
    updatedSchemaFieldTagUrns.addAll(tagUrns);

    // Override the current ones.
    updatedSchemaFieldProposal.setProposedSchemaTags(new UrnArray(updatedSchemaFieldTagUrns));

    // Filter out the previous proposals for the current field, add in the new one.
    final List<SchemaProposal> finalSchemaProposals =
        schemaProposals.getSchemaProposals().stream()
            .filter(proposal -> !schemaFieldPath.equals(proposal.getFieldPath()))
            .collect(Collectors.toList());
    finalSchemaProposals.add(updatedSchemaFieldProposal);

    final MetadataChangeProposal schemaProposalsMcp =
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, Constants.DATASET_SCHEMA_PROPOSALS_ASPECT_NAME, schemaProposals);

    this.entityClient.ingestProposal(opContext, schemaProposalsMcp, false);
  }

  private void addTagToEntityProposalsAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> tagUrns)
      throws RemoteInvocationException {
    final Proposals entityProposals =
        getEntityProposalsOrDefault(opContext, entityUrn, new Proposals());

    if (!entityProposals.hasProposedTags()) {
      entityProposals.setProposedTags(new UrnArray());
    }

    final Set<Urn> updatedTagUrns = new HashSet<>(entityProposals.getProposedTags());

    // Add the new tags to existing ones.
    updatedTagUrns.addAll(tagUrns);

    // Override the current ones.
    entityProposals.setProposedTags(new UrnArray(updatedTagUrns));

    final MetadataChangeProposal schemaProposalsMcp =
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, ENTITY_PROPOSALS_ASPECT_NAME, entityProposals);

    this.entityClient.ingestProposal(opContext, schemaProposalsMcp, false);
  }

  /*--------------------------------------------------------------------------
   *             GET PROPOSED TAGS HELPER METHODS
   *------------------------------------------------------------------------*/
  @Nullable
  private SchemaProposals getSchemaFieldProposalsOrDefault(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nullable SchemaProposals defaultValue) {
    SchemaProposals maybeSchemaProposals = getSchemaProposalsAspect(opContext, entityUrn);
    if (maybeSchemaProposals != null) {
      return maybeSchemaProposals;
    }
    return defaultValue;
  }

  @Nullable
  private SchemaProposals getSchemaProposalsAspect(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getSchemaProposalsEntityResponse(opContext, entityUrn);
    if (response != null
        && response.getAspects().containsKey(DATASET_SCHEMA_PROPOSALS_ASPECT_NAME)) {
      return new SchemaProposals(
          response.getAspects().get(DATASET_SCHEMA_PROPOSALS_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  @Nullable
  private EntityResponse getSchemaProposalsEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(DATASET_SCHEMA_PROPOSALS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve schema proposals for entity with urn %s", entityUrn),
          e);
    }
  }

  @Nullable
  private Proposals getEntityProposalsOrDefault(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nullable Proposals defaultValue) {
    Proposals maybeProposals = getEntityProposalsAspect(opContext, entityUrn);
    if (maybeProposals != null) {
      return maybeProposals;
    }
    return defaultValue;
  }

  @Nullable
  private Proposals getEntityProposalsAspect(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getEntityProposalsEntityResponse(opContext, entityUrn);
    if (response != null && response.getAspects().containsKey(ENTITY_PROPOSALS_ASPECT_NAME)) {
      return new Proposals(
          response.getAspects().get(ENTITY_PROPOSALS_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  @Nullable
  private EntityResponse getEntityProposalsEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(ENTITY_PROPOSALS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve entity proposals for entity with urn %s", entityUrn),
          e);
    }
  }

  private List<Urn> getProposedTagsForEntity(
      @Nonnull final OperationContext opContext, @Nonnull final Urn entityUrn)
      throws RemoteInvocationException, URISyntaxException {
    final Filter filter =
        createActionRequestFilter(
            TAG_ASSOCIATION_PROPOSAL_TYPE,
            ACTION_REQUEST_STATUS_PENDING,
            entityUrn.toString(),
            null,
            null);
    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
        .flatMap(
            actionRequestInfo ->
                actionRequestInfo.getParams().getTagProposal().hasTags()
                    ? actionRequestInfo.getParams().getTagProposal().getTags().stream()
                    : ImmutableList.of(actionRequestInfo.getParams().getTagProposal().getTag())
                        .stream())
        .collect(Collectors.toList());
  }

  private List<Urn> getProposedTagsForSchemaField(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath)
      throws RemoteInvocationException, URISyntaxException {
    final Filter filter =
        createActionRequestFilter(
            TAG_ASSOCIATION_PROPOSAL_TYPE,
            ACTION_REQUEST_STATUS_PENDING,
            entityUrn.toString(),
            schemaFieldPath,
            null);
    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
        .filter(actionRequestInfo -> schemaFieldPath.equals(actionRequestInfo.getSubResource()))
        .flatMap(
            actionRequestInfo ->
                actionRequestInfo.getParams().getTagProposal().hasTags()
                    ? actionRequestInfo.getParams().getTagProposal().getTags().stream()
                    : ImmutableList.of(actionRequestInfo.getParams().getTagProposal().getTag())
                        .stream())
        .collect(Collectors.toList());
  }

  private static Filter createActionRequestFilter(
      final @Nullable String type,
      final @Nullable String status,
      final @Nonnull String targetUrn,
      final @Nullable String targetSubresource,
      final @Nullable String result) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();
    if (status != null) {
      andCriterion.add(createStatusCriterion(status));
    }
    if (type != null) {
      andCriterion.add(createTypeCriterion(type));
    }
    if (targetSubresource != null) {
      andCriterion.add(createSubResourceCriterion(targetSubresource));
    }
    if (result != null) {
      andCriterion.add(createResultCriterion(result));
    }
    andCriterion.add(createResourceCriterion(targetUrn));

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);

    filter.setOr(disjunction);
    return filter;
  }

  private static Stream<ActionRequestInfo> getActionRequestInfosFromFilter(
      @Nonnull OperationContext opContext, Filter filter, EntityClient entityClient)
      throws RemoteInvocationException, URISyntaxException {
    final SearchResult searchResult =
        entityClient.filter(opContext, ACTION_REQUEST_ENTITY_NAME, filter, null, 0, 20);

    final Map<Urn, EntityResponse> entities =
        entityClient.batchGetV2(
            opContext,
            ACTION_REQUEST_ENTITY_NAME,
            new HashSet<>(
                searchResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toList())),
            ImmutableSet.of(
                Constants.ACTION_REQUEST_INFO_ASPECT_NAME,
                Constants.ACTION_REQUEST_STATUS_ASPECT_NAME));

    return entities.values().stream()
        .filter(
            entityResponse ->
                entityResponse.hasAspects()
                    && entityResponse
                        .getAspects()
                        .containsKey(Constants.ACTION_REQUEST_INFO_ASPECT_NAME)
                    && entityResponse.getAspects().get(Constants.ACTION_REQUEST_STATUS_ASPECT_NAME)
                        != null)
        .map(
            entity ->
                new ActionRequestInfo(
                    entity
                        .getAspects()
                        .get(Constants.ACTION_REQUEST_INFO_ASPECT_NAME)
                        .getValue()
                        .data()));
  }

  private static ActionRequestStatus createPendingActionRequestStatus(@Nonnull final Urn actor) {
    final ActionRequestStatus status = new ActionRequestStatus();

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    status.setStatus(ACTION_REQUEST_STATUS_PENDING);
    status.setLastModified(auditStamp);
    return status;
  }

  private static ActionRequestInfo createActionRequestInfo(
      @Nonnull final String type,
      @Nonnull final Urn entityUrn,
      @Nullable SubResourceType subResourceType,
      @Nullable String subResource,
      @Nonnull final ActionRequestParams params,
      @Nullable ActionRequestOrigin origin,
      @Nullable InferenceMetadata inferenceMetadata,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final Urn creator,
      @Nullable final String description) {

    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(type);
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setAssignedRoles(new UrnArray(assignedRoles));
    info.setResourceType(entityUrn.getEntityType());
    info.setResource(entityUrn.toString());
    if (subResourceType != null) {
      info.setSubResourceType(subResourceType.toString());
    }
    if (subResource != null) {
      info.setSubResource(subResource);
    }
    if (origin != null) {
      info.setOrigin(com.linkedin.actionrequest.ActionRequestOrigin.valueOf(origin.toString()));
    }
    if (inferenceMetadata != null) {
      info.setInferenceMetadata(inferenceMetadata);
    }
    info.setParams(params);
    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(creator);
    if (description != null) {
      info.setDescription(description);
    }

    return info;
  }

  private static ActionRequestParams createTagAssociationActionRequestParams(
      @Nonnull final List<Urn> tagUrns) {
    final ActionRequestParams params = new ActionRequestParams();
    params.setTagProposal(new TagProposal().setTags(new UrnArray(tagUrns)));
    return params;
  }

  /*--------------------------------------------------------------------------
   *                   VALIDATION METHODS FOR STRUCTURED PROPERTIES
   *------------------------------------------------------------------------*/
  private void validateProposeAssetStructuredProperties(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<StructuredPropertyValueAssignment> structuredPropertyValueAssignments)
      throws EntityDoesNotExistException,
          RemoteInvocationException,
          MalformedActionRequestException {

    // Step 1: Check if the entity exists
    if (!this.entityClient.exists(opContext, entityUrn, false)) {
      throw new EntityDoesNotExistException(String.format("Entity %s does not exist", entityUrn));
    }

    // Step 2: Check if the properties all exist
    for (StructuredPropertyValueAssignment valueAssignment : structuredPropertyValueAssignments) {
      if (!this.entityClient.exists(opContext, valueAssignment.getPropertyUrn(), false)) {
        throw new EntityDoesNotExistException(
            String.format(
                "Structured property with urn %s does not exist",
                valueAssignment.getPropertyUrn()));
      }
    }

    // Step 3: Validate the property values against the current definition
    for (StructuredPropertyValueAssignment valueAssignment : structuredPropertyValueAssignments) {
      if (!areProposedStructuredPropertyValuesValid(
          opContext, valueAssignment.getPropertyUrn(), valueAssignment.getValues())) {
        throw new MalformedActionRequestException(
            String.format(
                "Invalid values %s provided for structured property with urn %s",
                valueAssignment.getValues(), valueAssignment.getPropertyUrn()));
      }
    }

    // Step 4: Validate that same property is not proposed multiple times
    final Set<Urn> propertyUrns =
        structuredPropertyValueAssignments.stream()
            .map(StructuredPropertyValueAssignment::getPropertyUrn)
            .collect(Collectors.toSet());
    if (propertyUrns.size() != structuredPropertyValueAssignments.size()) {
      throw new MalformedActionRequestException(
          "Same structured property is proposed multiple times. Only one proposal is allowed for each property.");
    }
  }

  private boolean areProposedStructuredPropertyValuesValid(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn propertyUrn,
      @Nonnull final List<PrimitivePropertyValue> values) {
    return this.structuredPropertyService.areProposedStructuredPropertyValuesValid(
        opContext, propertyUrn, values);
  }

  private void validateProposeSchemaFieldStructuredProperties(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath,
      @Nonnull final List<StructuredPropertyValueAssignment> structuredPropertyValueAssignments)
      throws EntityDoesNotExistException,
          MalformedActionRequestException,
          RemoteInvocationException {

    // Step 1: Check if the entity exists
    if (!this.entityClient.exists(opContext, entityUrn, false)) {
      throw new EntityDoesNotExistException(String.format("Entity %s does not exist", entityUrn));
    }

    // Step 2: Check if the field path exists
    if (!this.datasetService.schemaFieldExists(opContext, entityUrn, schemaFieldPath)) {
      throw new EntityDoesNotExistException(
          String.format("Field path %s does not exist in entity %s", schemaFieldPath, entityUrn));
    }

    // Step 3: Check if the structured properties exist
    for (StructuredPropertyValueAssignment valueAssignment : structuredPropertyValueAssignments) {
      if (!this.entityClient.exists(opContext, valueAssignment.getPropertyUrn(), false)) {
        throw new EntityDoesNotExistException(
            String.format(
                "Structured Property with urn %s does not exist",
                valueAssignment.getPropertyUrn()));
      }
    }

    // Step 4: Ensure that the entity urn is a dataset urn when proposing for a schema field
    if (!entityUrn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
      throw new MalformedActionRequestException("Entity urn must be a dataset urn");
    }

    // Step 5: Validate the property values against the current definition
    for (StructuredPropertyValueAssignment valueAssignment : structuredPropertyValueAssignments) {
      if (!areProposedStructuredPropertyValuesValid(
          opContext, valueAssignment.getPropertyUrn(), valueAssignment.getValues())) {
        throw new MalformedActionRequestException(
            String.format(
                "Invalid values %s provided for structured property with urn %s",
                valueAssignment.getValues(), valueAssignment.getPropertyUrn()));
      }
    }

    // Step 6: Validate that same property is not proposed multiple times
    final Set<Urn> propertyUrns =
        structuredPropertyValueAssignments.stream()
            .map(StructuredPropertyValueAssignment::getPropertyUrn)
            .collect(Collectors.toSet());
    if (propertyUrns.size() != structuredPropertyValueAssignments.size()) {
      throw new MalformedActionRequestException(
          "Same structured property is proposed multiple times. Only one proposal is allowed for each property.");
    }
  }

  /*--------------------------------------------------------------------------
   *                     FILTER METHODS FOR STRUCTURED PROPERTIES
   *------------------------------------------------------------------------*/
  private List<StructuredPropertyValueAssignment> filterEntityStructuredPropertiesToPropose(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<StructuredPropertyValueAssignment> structuredPropertyValueAssignments)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          RemoteInvocationException,
          URISyntaxException {

    // Get the currently-applied properties
    final List<StructuredPropertyValueAssignment> appliedProperties =
        this.structuredPropertyService.getEntityStructuredProperties(opContext, entityUrn);

    // Get the structured properties already proposed
    final List<StructuredPropertyValueAssignment> proposedProperties =
        getProposedStructuredPropertiesForEntity(opContext, entityUrn);

    // Deep check whether all are already applied
    if (appliedProperties.size() == structuredPropertyValueAssignments.size()
        && propertiesAreEqual(appliedProperties, structuredPropertyValueAssignments)) {
      throw new AlreadyAppliedException(
          "All structured property values are already applied to the entity");
    }

    // Deep check whether all are already proposed
    if (proposedProperties.size() == structuredPropertyValueAssignments.size()
        && propertiesAreEqual(proposedProperties, structuredPropertyValueAssignments)) {
      throw new AlreadyRequestedException(
          "All structured properties are already proposed for the entity");
    }

    // Filter out property values that are either already applied or already proposed
    return structuredPropertyValueAssignments.stream()
        .filter(
            propertyValueAssignment ->
                appliedProperties.stream()
                    .noneMatch(
                        appliedProperty ->
                            propertyIsEqual(appliedProperty, propertyValueAssignment)))
        .filter(
            propertyValueAssignment ->
                proposedProperties.stream()
                    .noneMatch(
                        proposedProperty ->
                            propertyIsEqual(proposedProperty, propertyValueAssignment)))
        .collect(Collectors.toList());
  }

  private List<StructuredPropertyValueAssignment> filterSchemaFieldStructuredPropertiesToPropose(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<StructuredPropertyValueAssignment> structuredPropertyValueAssignments,
      @Nonnull final String schemaFieldPath)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          RemoteInvocationException,
          URISyntaxException {

    // Get the currently-applied properties on the field
    final List<StructuredPropertyValueAssignment> appliedProperties =
        this.structuredPropertyService.getSchemaFieldStructuredProperties(
            opContext, entityUrn, schemaFieldPath);

    // Get the structured properties already proposed
    final List<StructuredPropertyValueAssignment> proposedProperties =
        getProposedStructuredPropertiesForSchemaField(opContext, entityUrn, schemaFieldPath);

    // Deep check whether all are already applied
    if (appliedProperties.size() == structuredPropertyValueAssignments.size()
        && propertiesAreEqual(appliedProperties, structuredPropertyValueAssignments)) {
      throw new AlreadyAppliedException(
          String.format(
              "All structured property values are already applied to the schema field with path %s",
              schemaFieldPath));
    }

    // Deep check whether all are already proposed
    if (proposedProperties.size() == structuredPropertyValueAssignments.size()
        && propertiesAreEqual(proposedProperties, structuredPropertyValueAssignments)) {
      throw new AlreadyRequestedException(
          String.format(
              "All structured properties are already proposed for the schema field with path %s",
              schemaFieldPath));
    }

    // Filter out property values that are either already applied or already proposed
    return structuredPropertyValueAssignments.stream()
        .filter(
            propertyValueAssignment ->
                appliedProperties.stream()
                    .noneMatch(
                        appliedProperty ->
                            propertyIsEqual(appliedProperty, propertyValueAssignment)))
        .filter(
            propertyValueAssignment ->
                proposedProperties.stream()
                    .noneMatch(
                        proposedProperty ->
                            propertyIsEqual(proposedProperty, propertyValueAssignment)))
        .collect(Collectors.toList());
  }

  private boolean propertiesAreEqual(
      @Nonnull final List<StructuredPropertyValueAssignment> properties1,
      @Nonnull final List<StructuredPropertyValueAssignment> properties2) {
    return properties1.stream()
        .allMatch(
            property1 ->
                properties2.stream().anyMatch(property2 -> propertyIsEqual(property1, property2)));
  }

  private boolean propertyIsEqual(
      @Nonnull final StructuredPropertyValueAssignment property1,
      @Nonnull final StructuredPropertyValueAssignment property2) {
    return property1.getPropertyUrn().equals(property2.getPropertyUrn())
        && property1.getValues().equals(property2.getValues());
  }

  /*--------------------------------------------------------------------------
   *             GET PROPOSED STRUCTURED PROPERTIES HELPER METHODS
   *------------------------------------------------------------------------*/

  private List<StructuredPropertyValueAssignment> getProposedStructuredPropertiesForEntity(
      @Nonnull final OperationContext opContext, @Nonnull final Urn entityUrn)
      throws RemoteInvocationException, URISyntaxException {

    final Filter filter =
        createActionRequestFilter(
            STRUCTURED_PROPERTY_ASSOCIATION_PROPOSAL_TYPE,
            ACTION_REQUEST_STATUS_PENDING,
            entityUrn.toString(),
            null,
            null);

    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
        .flatMap(
            actionRequestInfo ->
                actionRequestInfo.getParams().hasStructuredPropertyProposal()
                    ? actionRequestInfo
                        .getParams()
                        .getStructuredPropertyProposal()
                        .getStructuredPropertyValues()
                        .stream()
                    : Stream.empty())
        .collect(Collectors.toList());
  }

  private List<StructuredPropertyValueAssignment> getProposedStructuredPropertiesForSchemaField(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String schemaFieldPath)
      throws RemoteInvocationException, URISyntaxException {

    final Filter filter =
        createActionRequestFilter(
            STRUCTURED_PROPERTY_ASSOCIATION_PROPOSAL_TYPE,
            ACTION_REQUEST_STATUS_PENDING,
            entityUrn.toString(),
            schemaFieldPath,
            null);

    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
        .filter(actionRequestInfo -> schemaFieldPath.equals(actionRequestInfo.getSubResource()))
        .flatMap(
            actionRequestInfo ->
                actionRequestInfo.getParams().hasStructuredPropertyProposal()
                    ? actionRequestInfo
                        .getParams()
                        .getStructuredPropertyProposal()
                        .getStructuredPropertyValues()
                        .stream()
                    : Stream.empty())
        .collect(Collectors.toList());
  }

  /*--------------------------------------------------------------------------
   *             CREATION OF ACTION REQUESTS FOR STRUCTURED PROPERTIES
   *------------------------------------------------------------------------*/
  private Urn createStructuredPropertyAssociationActionRequest(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<StructuredPropertyValueAssignment> propertyValueAssignments,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nullable final String description)
      throws RemoteInvocationException {

    // First, get the assignees for the structured property change proposal.
    final AuthorizedActors assignees =
        getStructuredPropertyAssociationAssignees(opContext, entityUrn, subResourceType);

    // Then, create the action request
    final List<MetadataChangeProposal> mcps =
        createStructuredPropertyAssociationActionRequestMcps(
            entityUrn,
            propertyValueAssignments,
            subResourceType,
            subResource,
            assignees.getUsers(),
            assignees.getGroups(),
            assignees.getRoles(),
            opContext.getAuditStamp().getActor(),
            description);

    // Finally, ingest the aspects!
    this.entityClient.batchIngestProposals(opContext, mcps, false);

    // TODO: Determine whether we need a secondary aspect for indexing.

    // Return the URN of the new action request.
    return mcps.get(0).getEntityUrn();
  }

  @Nonnull
  private List<MetadataChangeProposal> createStructuredPropertyAssociationActionRequestMcps(
      @Nonnull final Urn entityUrn,
      @Nonnull final List<StructuredPropertyValueAssignment> propertyValueAssignments,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final Urn actorUrn,
      @Nullable final String description) {
    return createActionRequestMcps(
        STRUCTURED_PROPERTY_ASSOCIATION_PROPOSAL_TYPE,
        entityUrn,
        subResourceType,
        subResource,
        createStructuredPropertyAssociationActionRequestParams(propertyValueAssignments),
        assignedUsers,
        assignedGroups,
        assignedRoles,
        actorUrn,
        description);
  }

  @Nonnull
  private ActionRequestParams createStructuredPropertyAssociationActionRequestParams(
      @Nonnull final List<StructuredPropertyValueAssignment> propertyValueAssignments) {
    final ActionRequestParams params = new ActionRequestParams();
    StructuredPropertyProposal structuredPropertyProposal = new StructuredPropertyProposal();
    structuredPropertyProposal.setStructuredPropertyValues(
        new StructuredPropertyValueAssignmentArray(propertyValueAssignments));
    params.setStructuredPropertyProposal(structuredPropertyProposal);
    return params;
  }

  @Nonnull
  private AuthorizedActors getStructuredPropertyAssociationAssignees(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nullable final SubResourceType subResourceType) {

    EntitySpec spec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());
    if (SubResourceType.DATASET_FIELD.equals(subResourceType)) {
      return opContext
          .getAuthorizationContext()
          .getAuthorizer()
          .authorizedActors(
              PoliciesConfig.MANAGE_DATASET_COL_PROPERTIES_PRIVILEGE.getType(), Optional.of(spec));
    }
    // Otherwise, handle top-level entity properties
    return opContext
        .getAuthorizationContext()
        .getAuthorizer()
        .authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_PROPERTIES_PRIVILEGE.getType(), Optional.of(spec));
  }

  /*--------------------------------------------------------------------------
   *                   VALIDATION METHODS FOR DOMAINS
   *------------------------------------------------------------------------*/
  private void validateProposeAssetDomain(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn domainUrn)
      throws EntityDoesNotExistException, RemoteInvocationException {

    // Step 1: Check if the entity exists
    if (!this.entityClient.exists(opContext, entityUrn, false)) {
      throw new EntityDoesNotExistException(String.format("Entity %s does not exist", entityUrn));
    }

    // Step 2: Check if the domain exist
    if (!this.entityClient.exists(opContext, domainUrn, false)) {
      throw new EntityDoesNotExistException(
          String.format("Domain with urn %s does not exist", domainUrn));
    }
  }

  /*--------------------------------------------------------------------------
   *                     FILTER METHODS FOR DOMAINS
   *------------------------------------------------------------------------*/
  @Nonnull
  private Urn filterEntityDomainToPropose(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn domainUrn)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          RemoteInvocationException,
          URISyntaxException {

    // Get the currently-applied Domain
    final List<Urn> appliedDomains = this.domainService.getEntityDomains(opContext, entityUrn);

    // Get the Domains already proposed
    final List<Urn> proposedDomains = getProposedDomainsForEntity(opContext, entityUrn);

    // Deep check whether all are already applied
    if (appliedDomains.contains(domainUrn)) {
      throw new AlreadyAppliedException("Domain is already applied to the entity");
    }

    // Deep check whether all are already proposed
    if (proposedDomains.contains(domainUrn)) {
      throw new AlreadyRequestedException("Domain is already proposed for the entity");
    }

    return domainUrn;
  }

  /*--------------------------------------------------------------------------
   *             CREATION OF ACTION REQUESTS FOR DOMAIN
   *------------------------------------------------------------------------*/
  private Urn createDomainAssociationActionRequest(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn domainUrn,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nullable final String description)
      throws RemoteInvocationException {

    // First, get the assignees (authorized actors) for the domain change proposal.
    final AuthorizedActors assignees = getDomainAssociationAssignees(opContext, entityUrn);

    // Then, create the action request
    final List<MetadataChangeProposal> mcps =
        createDomainAssociationActionRequestMcps(
            entityUrn,
            domainUrn,
            subResourceType,
            subResource,
            assignees.getUsers(),
            assignees.getGroups(),
            assignees.getRoles(),
            opContext.getAuditStamp().getActor(),
            description);

    // Ingest the aspects
    this.entityClient.batchIngestProposals(opContext, mcps, false);

    // Return the URN of the new action request.
    return mcps.get(0).getEntityUrn();
  }

  @Nonnull
  private List<MetadataChangeProposal> createDomainAssociationActionRequestMcps(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn domainUrn,
      @Nullable final SubResourceType subResourceType,
      @Nullable final String subResource,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final Urn actorUrn,
      @Nullable final String description) {

    return createActionRequestMcps(
        DOMAIN_ASSOCIATION_PROPOSAL_TYPE,
        entityUrn,
        subResourceType,
        subResource,
        createDomainAssociationActionRequestParams(domainUrn),
        assignedUsers,
        assignedGroups,
        assignedRoles,
        actorUrn,
        description);
  }

  @Nonnull
  private ActionRequestParams createDomainAssociationActionRequestParams(
      @Nonnull final Urn domainUrn) {
    final ActionRequestParams params = new ActionRequestParams();
    DomainProposal domainProposal = new DomainProposal();
    domainProposal.setDomains(new UrnArray(ImmutableList.of(domainUrn)));
    params.setDomainProposal(domainProposal);
    return params;
  }

  /*--------------------------------------------------------------------------
   *             GET PROPOSED DOMAIN HELPER METHODS
   *------------------------------------------------------------------------*/

  private List<Urn> getProposedDomainsForEntity(
      @Nonnull final OperationContext opContext, @Nonnull final Urn entityUrn)
      throws RemoteInvocationException, URISyntaxException {

    final Filter filter =
        createActionRequestFilter(
            DOMAIN_ASSOCIATION_PROPOSAL_TYPE,
            ACTION_REQUEST_STATUS_PENDING,
            entityUrn.toString(),
            null,
            null);

    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
        .flatMap(
            actionRequestInfo ->
                actionRequestInfo.getParams().getDomainProposal().getDomains().stream())
        .collect(Collectors.toList());
  }

  @Nonnull
  private AuthorizedActors getDomainAssociationAssignees(
      @Nonnull final OperationContext opContext, @Nonnull final Urn entityUrn) {

    EntitySpec spec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());
    return opContext
        .getAuthorizationContext()
        .getAuthorizer()
        .authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_DOMAINS_PRIVILEGE.getType(), Optional.of(spec));
  }

  /*--------------------------------------------------------------------------
   *                   VALIDATION METHODS FOR OWNERS
   *------------------------------------------------------------------------*/
  private void validateProposeAssetOwners(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Owner> owners)
      throws EntityDoesNotExistException, RemoteInvocationException {

    // Step 1: Check if the entity exists
    if (!this.entityClient.exists(opContext, entityUrn, false)) {
      throw new EntityDoesNotExistException(String.format("Entity %s does not exist", entityUrn));
    }

    for (final Owner owner : owners) {
      // Step 2: Check if the owners exist
      if (!this.entityClient.exists(opContext, owner.getOwner(), false)) {
        throw new EntityDoesNotExistException(
            String.format("Owner with urn %s does not exist", owner.getOwner()));
      }
      // Step 3: Check if the owner type exists
      if (owner.hasTypeUrn()) {
        if (!this.entityClient.exists(opContext, owner.getTypeUrn(), false)) {
          throw new EntityDoesNotExistException(
              String.format("Owner type with urn %s does not exist", owner.getTypeUrn()));
        }
      }
    }
  }

  /*--------------------------------------------------------------------------
   *                     FILTER METHODS FOR OWNERS
   *------------------------------------------------------------------------*/
  @Nonnull
  private List<Owner> filterEntityOwnersToPropose(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Owner> owners)
      throws AlreadyAppliedException,
          AlreadyRequestedException,
          RemoteInvocationException,
          URISyntaxException {

    // Get the currently-applied owners
    final List<Owner> appliedOwners = this.ownerService.getEntityOwners(opContext, entityUrn);

    // Get the owners already proposed
    final List<Owner> proposedOwners = getProposedOwnersForEntity(opContext, entityUrn);

    // Deep check whether all are already applied
    if (appliedOwners.size() == owners.size() && ownersAreEqual(appliedOwners, owners)) {
      throw new AlreadyAppliedException("All owners are already applied to the entity");
    }

    // Deep check whether all are already proposed
    if (proposedOwners.size() == owners.size() && ownersAreEqual(proposedOwners, owners)) {
      throw new AlreadyRequestedException("All owners are already proposed for the entity");
    }

    // Filter out owner values that are either already applied or already proposed
    return owners.stream()
        .filter(
            owner ->
                appliedOwners.stream().noneMatch(appliedOwner -> ownerIsEqual(appliedOwner, owner)))
        .filter(
            owner ->
                proposedOwners.stream()
                    .noneMatch(proposedOwner -> ownerIsEqual(proposedOwner, owner)))
        .collect(Collectors.toList());
  }

  private boolean ownersAreEqual(
      @Nonnull final List<Owner> owners1, @Nonnull final List<Owner> owners2) {
    return owners1.stream()
        .allMatch(owner1 -> owners2.stream().anyMatch(owner2 -> ownerIsEqual(owner1, owner2)));
  }

  private boolean ownerIsEqual(@Nonnull final Owner owner1, @Nonnull final Owner owner2) {
    return owner1.getOwner().equals(owner2.getOwner())
        && owner1.getType().equals(owner2.getType())
        // If both owners have type urns, then they must be equal
        && (owner1.hasTypeUrn() && owner2.hasTypeUrn()
            ? owner1.getTypeUrn().equals(owner2.getTypeUrn())
            : true);
  }

  /*--------------------------------------------------------------------------
   *             CREATION OF ACTION REQUESTS FOR OWNERS
   *------------------------------------------------------------------------*/
  private Urn createOwnersAssociationActionRequest(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Owner> owners,
      @Nullable final String description)
      throws RemoteInvocationException {

    // First, get the assignees (authorized actors) for the owners change proposal.
    final AuthorizedActors assignees = getOwnersAssociationAssignees(opContext, entityUrn);

    // Then, create the action request
    final List<MetadataChangeProposal> mcps =
        createOwnersAssociationActionRequestMcps(
            entityUrn,
            owners,
            assignees.getUsers(),
            assignees.getGroups(),
            assignees.getRoles(),
            opContext.getAuditStamp().getActor(),
            description);

    // Ingest the aspects
    this.entityClient.batchIngestProposals(opContext, mcps, false);

    // Return the URN of the new action request.
    return mcps.get(0).getEntityUrn();
  }

  @Nonnull
  private List<MetadataChangeProposal> createOwnersAssociationActionRequestMcps(
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Owner> owners,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final Urn actorUrn,
      @Nullable final String description) {

    return createActionRequestMcps(
        OWNER_ASSOCIATION_PROPOSAL_TYPE,
        entityUrn,
        null,
        null,
        createOwnersAssociationActionRequestParams(owners),
        assignedUsers,
        assignedGroups,
        assignedRoles,
        actorUrn,
        description);
  }

  @Nonnull
  private ActionRequestParams createOwnersAssociationActionRequestParams(
      @Nonnull final List<Owner> owners) {
    final ActionRequestParams params = new ActionRequestParams();
    OwnerProposal ownerProposal = new OwnerProposal();
    ownerProposal.setOwners(new OwnerArray(owners));
    params.setOwnerProposal(ownerProposal);
    return params;
  }

  /*--------------------------------------------------------------------------
   *             GET PROPOSED OWNERS HELPER METHODS
   *------------------------------------------------------------------------*/

  private List<Owner> getProposedOwnersForEntity(
      @Nonnull final OperationContext opContext, @Nonnull final Urn entityUrn)
      throws RemoteInvocationException, URISyntaxException {

    final Filter filter =
        createActionRequestFilter(
            OWNER_ASSOCIATION_PROPOSAL_TYPE,
            ACTION_REQUEST_STATUS_PENDING,
            entityUrn.toString(),
            null,
            null);

    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
        .flatMap(
            actionRequestInfo ->
                actionRequestInfo.getParams().getOwnerProposal().getOwners().stream())
        .collect(Collectors.toList());
  }

  @Nonnull
  private AuthorizedActors getOwnersAssociationAssignees(
      @Nonnull final OperationContext opContext, @Nonnull final Urn entityUrn) {

    EntitySpec spec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());
    return opContext
        .getAuthorizationContext()
        .getAuthorizer()
        .authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_OWNERS_PRIVILEGE.getType(), Optional.of(spec));
  }

  /*--------------------------------------------------------------------------
   *            ACCEPT DOMAIN PROPOSAL
   *------------------------------------------------------------------------*/

  /**
   * Accept a domain proposal by urn. Assumes that the authorization has ALREADY been validated
   * outside (e.g. in a resolver).
   *
   * @param opContext the operation context
   * @param actionRequestUrn the action request urn
   * @throws Exception if the proposal cannot be accepted
   */
  public void acceptDomainProposal(
      @Nonnull OperationContext opContext, @Nonnull final Urn actionRequestUrn)
      throws MalformedActionRequestException, RemoteInvocationException {
    Objects.requireNonNull(actionRequestUrn, "actionRequestUrn cannot be null");
    final ActionRequestInfo maybeInfo = getActionRequestInfo(opContext, actionRequestUrn);
    if (maybeInfo == null) {
      throw new EntityDoesNotExistException(
          String.format("Action request with urn %s does not exist.", actionRequestUrn));
    }
    acceptDomainProposal(opContext, maybeInfo);
  }

  /**
   * Accept a domain proposal. Assumes that the authorization has ALREADY been validated outside
   * (e.g. in a resolver).
   *
   * @param opContext the operation context
   * @param actionRequestInfo the action request info
   */
  public void acceptDomainProposal(
      @Nonnull final OperationContext opContext, @Nonnull final ActionRequestInfo actionRequestInfo)
      throws RemoteInvocationException, MalformedActionRequestException {

    if (!ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL.equals(actionRequestInfo.getType())) {
      throw new MalformedActionRequestException("Action request is not a Domain proposal");
    }

    if (!actionRequestInfo.hasParams() || !actionRequestInfo.getParams().hasDomainProposal()) {
      throw new MalformedActionRequestException("Action request does not contain domain proposal");
    }

    final DomainProposal domainProposal = actionRequestInfo.getParams().getDomainProposal();
    final List<Urn> domainUrns = domainProposal.getDomains();

    if (domainUrns.size() > 1) {
      throw new MalformedActionRequestException(
          "Propose domain action request should contain only one domain urn");
    }

    final Urn entityUrn = UrnUtils.getUrn(actionRequestInfo.getResource());

    if (domainUrns.size() == 1) {
      this.domainService.setDomain(opContext, entityUrn, domainUrns.get(0));
    } else {
      this.domainService.unsetDomain(opContext, entityUrn);
    }
  }

  /*--------------------------------------------------------------------------
   *            ACCEPT OWNER PROPOSAL
   *------------------------------------------------------------------------*/

  /**
   * Accept a owner proposal by urn. Assumes that the authorization has ALREADY been validated
   * outside (e.g. in a resolver).
   *
   * @param opContext the operation context
   * @param actionRequestUrn the action request urn
   * @throws Exception if the proposal cannot be accepted
   */
  public void acceptOwnerProposal(
      @Nonnull OperationContext opContext, @Nonnull final Urn actionRequestUrn) throws Exception {
    Objects.requireNonNull(actionRequestUrn, "actionRequestUrn cannot be null");
    final ActionRequestInfo maybeInfo = getActionRequestInfo(opContext, actionRequestUrn);
    if (maybeInfo == null) {
      throw new EntityDoesNotExistException(
          String.format("Action request with urn %s does not exist.", actionRequestUrn));
    }
    acceptOwnerProposal(opContext, maybeInfo);
  }

  /**
   * Accept an owner proposal. Assumes that the authorization has ALREADY been validated outside
   * (e.g. in a resolver).
   *
   * @param opContext the operation context
   * @param actionRequestInfo the action request info
   */
  public void acceptOwnerProposal(
      @Nonnull final OperationContext opContext, @Nonnull final ActionRequestInfo actionRequestInfo)
      throws Exception {

    if (!ACTION_REQUEST_TYPE_OWNER_PROPOSAL.equals(actionRequestInfo.getType())) {
      throw new MalformedActionRequestException("Action request is not a owner proposal");
    }

    if (!actionRequestInfo.hasParams() || !actionRequestInfo.getParams().hasOwnerProposal()) {
      throw new MalformedActionRequestException("Action request does not contain owner proposal");
    }

    final OwnerProposal ownerProposal = actionRequestInfo.getParams().getOwnerProposal();
    final List<Owner> owners = ownerProposal.getOwners();

    final Urn entityUrn = UrnUtils.getUrn(actionRequestInfo.getResource());
    this.ownerService.addOwners(opContext, entityUrn, owners);
  }

  private static Criterion createStatusCriterion(String status) {
    return CriterionUtils.buildCriterion(STATUS_FIELD_NAME, Condition.EQUAL, status.toString());
  }

  private static Criterion createTypeCriterion(String type) {
    return CriterionUtils.buildCriterion(TYPE_FIELD_NAME, Condition.EQUAL, type.toString());
  }

  private static Criterion createResultCriterion(String result) {
    return CriterionUtils.buildCriterion(RESULT_FIELD_NAME, Condition.EQUAL, result.toString());
  }

  private static Criterion createResourceCriterion(String targetUrn) {
    return CriterionUtils.buildCriterion(RESOURCE_FIELD_NAME, Condition.EQUAL, targetUrn);
  }

  private static Criterion createSubResourceCriterion(String subResource) {
    return CriterionUtils.buildCriterion(SUBRESOURCE_FIELD_NAME, Condition.EQUAL, subResource);
  }

  private static Criterion createStartTimestampCriterion(Long startTimestampMillis) {
    return CriterionUtils.buildCriterion(
        LAST_MODIFIED_FIELD_NAME,
        Condition.GREATER_THAN_OR_EQUAL_TO,
        startTimestampMillis.toString());
  }

  private static Criterion createEndTimestampCriterion(Long endTimestampMillis) {
    return CriterionUtils.buildCriterion(
        LAST_MODIFIED_FIELD_NAME, Condition.LESS_THAN_OR_EQUAL_TO, endTimestampMillis.toString());
  }

  /**
   * Exception thrown when the changes requested as part of the action request are already applied
   * to the target resource.
   */
  public static class AlreadyAppliedException extends Exception {
    public AlreadyAppliedException(String message) {
      super(message);
    }
  }

  /**
   * Exception thrown when the changes requested as part of the action request are already requested
   * or raised as another action request.
   */
  public static class AlreadyRequestedException extends Exception {
    public AlreadyRequestedException(String message) {
      super(message);
    }
  }

  /**
   * Exception thrown when the action request is malformed. This can happen if: 1. The request is
   * missing required fields. 2. The request contains invalid values. For example, if tag urns are
   * expected by not provided. 3. The request contains invalid combinations of fields.
   */
  public static class MalformedActionRequestException extends Exception {
    public MalformedActionRequestException(String message) {
      super(message);
    }
  }

  @Value
  private static class AssignedActors {
    List<Urn> users;
    List<Urn> groups;
    List<Urn> roles;
  }

  @VisibleForTesting
  boolean isAuthorizedToResolveGlossaryEntityAsOwner(
      @Nonnull OperationContext opContext, @Nonnull Urn actorUrn, Optional<Urn> parentNode) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");

    if (!parentNode.isPresent()) {
      return false;
    }

    Pair<List<Urn>, List<Urn>> parentNodeOwners = getUrnOwners(opContext, parentNode.get());
    Set<Urn> userOwners = new HashSet<>(parentNodeOwners.getFirst());
    if (userOwners.contains(actorUrn)) {
      return true;
    }

    Set<Urn> groupOwners = new HashSet<>(parentNodeOwners.getSecond());
    GroupMembership groupMembership =
        (GroupMembership)
            this.entityService.getLatestAspect(opContext, actorUrn, GROUP_MEMBERSHIP_ASPECT_NAME);

    // TODO: Union with Native Group Membership
    Set<Urn> actorGroups = new HashSet<>(groupMembership.getGroups());
    actorGroups.retainAll(groupOwners);
    return !actorGroups.isEmpty();
  }

  private AssignedActors getAssignedUsersAndGroupsForGlossary(
      @Nonnull OperationContext opContext,
      @Nonnull final String privilegeType,
      @Nonnull final Optional<Urn> urnOptional) {
    AuthorizedActors authorizedActors =
        opContext
            .getAuthorizationContext()
            .getAuthorizer()
            .authorizedActors(privilegeType, Optional.empty());

    List<Urn> assignedUsers =
        authorizedActors == null ? new ArrayList<>() : authorizedActors.getUsers();
    List<Urn> assignedGroups =
        authorizedActors == null ? new ArrayList<>() : authorizedActors.getGroups();
    List<Urn> assignedRoles =
        authorizedActors == null ? new ArrayList<>() : authorizedActors.getRoles();
    if (urnOptional.isPresent()) {
      Ownership ownership =
          (Ownership)
              this.entityService.getLatestAspect(
                  opContext, urnOptional.get(), OWNERSHIP_ASPECT_NAME);
      if (ownership != null && ownership.hasOwners()) {
        Pair<List<Urn>, List<Urn>> urnOwners = getUrnOwners(opContext, urnOptional.get());
        List<Urn> userOwners = urnOwners.getFirst();
        List<Urn> groupOwners = urnOwners.getSecond();

        assignedUsers.addAll(userOwners);
        assignedGroups.addAll(groupOwners);
      }
    }
    return new AssignedActors(assignedUsers, assignedGroups, assignedRoles);
  }

  /*--------------------------------------------------------------------------
   *                     MISC UTILITY METHODS
   *------------------------------------------------------------------------*/
  @Nullable
  private Urn getDataContractUrn(@Nonnull OperationContext opContext, final Urn entityUrn) {
    EntityRelationships relationships =
        this.graphClient.getRelatedEntities(
            entityUrn.toString(),
            ImmutableList.of("ContractFor"),
            RelationshipDirection.INCOMING,
            0,
            1,
            opContext.getSessionAuthentication().getActor().toUrnStr());

    if (relationships.getTotal() > 1) {
      // Bad state - There are multiple contracts for a single entity! Cannot update.
      log.warn(
          String.format(
              "Found entity with multiple data contracts! urn: %s, num contracts: %s",
              entityUrn, relationships.getTotal()));
    }

    if (relationships.getRelationships().size() >= 1) {
      return relationships.getRelationships().get(0).getEntity();
    }
    return null;
  }

  @Nullable
  private DataContractProperties getDataContractProperties(
      @Nonnull OperationContext opContext, Urn contractUrn) {
    try {
      EntityResponse response =
          this.entityClient.getV2(
              opContext,
              DATA_CONTRACT_ENTITY_NAME,
              contractUrn,
              ImmutableSet.of(DATA_CONTRACT_PROPERTIES_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(DATA_CONTRACT_PROPERTIES_ASPECT_NAME)) {
        return new DataContractProperties(
            response.getAspects().get(DATA_CONTRACT_PROPERTIES_ASPECT_NAME).data());
      }
      return null;
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to retrieve data contract properties for contract urn %s", contractUrn));
      return null;
    }
  }

  @Nonnull
  private DataContractProperties mergeDataContractProperties(
      @Nonnull final DataContractProposalOperationType type,
      @Nonnull final DataContractProperties first,
      @Nonnull final DataContractProperties second) {
    final DataContractProperties properties = new DataContractProperties();
    final List<FreshnessContract> freshness = new ArrayList<>();
    final List<SchemaContract> schema = new ArrayList<>();
    final List<DataQualityContract> quality = new ArrayList<>();
    if (DataContractProposalOperationType.ADD.equals(type) && first.hasFreshness()) {
      freshness.addAll(first.getFreshness());
    }
    if (second.hasFreshness()) {
      freshness.addAll(second.getFreshness());
    }
    if (DataContractProposalOperationType.ADD.equals(type) && first.hasSchema()) {
      schema.addAll(first.getSchema());
    }
    if (second.hasSchema()) {
      schema.addAll(second.getSchema());
    }
    if (DataContractProposalOperationType.ADD.equals(type) && first.hasDataQuality()) {
      quality.addAll(first.getDataQuality());
    }
    if (second.hasDataQuality()) {
      quality.addAll(second.getDataQuality());
    }
    properties.setFreshness(new FreshnessContractArray(freshness));
    properties.setSchema(new SchemaContractArray(schema));
    properties.setDataQuality(new DataQualityContractArray(quality));
    properties.setEntity(second.getEntity());
    return properties;
  }

  private Pair<List<Urn>, List<Urn>> getUrnOwners(
      @Nonnull OperationContext opContext, @Nonnull final Urn urn) {
    Ownership ownership =
        (Ownership) this.entityService.getLatestAspect(opContext, urn, OWNERSHIP_ASPECT_NAME);
    List<Urn> userOwners =
        ownership.getOwners().stream()
            .map(Owner::getOwner)
            .filter(owner -> owner.getEntityType().equals(CORP_USER_ENTITY_NAME))
            .collect(Collectors.toList());
    List<Urn> groupOwners =
        ownership.getOwners().stream()
            .map(Owner::getOwner)
            .filter(owner -> owner.getEntityType().equals(CORP_GROUP_ENTITY_NAME))
            .collect(Collectors.toList());

    return Pair.of(userOwners, groupOwners);
  }

  public static ActionRequestInfo findActionRequestInfoAspect(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot) {
    Optional<ActionRequestAspect> actionRequestInfoOptional =
        actionRequestSnapshot.getAspects().stream()
            .filter(ActionRequestAspect::isActionRequestInfo)
            .findFirst();
    if (!actionRequestInfoOptional.isPresent()) {
      throw new RuntimeException("Could not find ActionRequestInfo");
    }
    ActionRequestAspect actionRequestInfoAspect = actionRequestInfoOptional.get();
    if (!actionRequestInfoAspect.isActionRequestInfo()) {
      throw new RuntimeException("ActionRequestAspect is not ActionRequestInfo");
    }
    ActionRequestInfo actionRequestInfo = actionRequestInfoAspect.getActionRequestInfo();
    if (!actionRequestInfo.hasParams()) {
      throw new RuntimeException("ActionRequestInfo does not have params");
    }
    return actionRequestInfo;
  }

  private static ActionRequestSnapshot setStatusSnapshot(
      @Nonnull final Urn actorUrn,
      @Nonnull final String actionRequestStatus,
      @Nonnull final String actionRequestResult,
      @Nullable final String note,
      @Nonnull final Entity proposalEntity) {
    ActionRequestStatus statusAspect =
        proposalEntity.getValue().getActionRequestSnapshot().getAspects().stream()
            .filter(ActionRequestAspect::isActionRequestStatus)
            .findFirst()
            .orElse(ActionRequestAspect.create(createActionRequestStatus(actorUrn)))
            .getActionRequestStatus();

    statusAspect.setStatus(actionRequestStatus);
    statusAspect.setResult(actionRequestResult);
    statusAspect.setNote(note, SetMode.IGNORE_NULL);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    statusAspect.setLastModified(auditStamp);

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(statusAspect));

    ActionRequestSnapshot snapshot = new ActionRequestSnapshot();
    snapshot.setUrn(proposalEntity.getValue().getActionRequestSnapshot().getUrn());
    snapshot.setAspects(aspects);

    return snapshot;
  }

  private static ActionRequestStatus createActionRequestStatus(@Nonnull final Urn actorUrn) {
    final ActionRequestStatus status = new ActionRequestStatus();

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    status.setStatus(ACTION_REQUEST_STATUS_PENDING);
    status.setResult(ACTION_REQUEST_EMPTY_RESULT);
    status.setLastModified(auditStamp);

    return status;
  }

  @VisibleForTesting
  static ActionRequestSnapshot createCreateGlossaryNodeProposalActionRequest(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description,
      @Nullable final String proposalNote) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(
        ActionRequestAspect.create(
            createCreateGlossaryNodeActionRequestInfo(
                actorUrn,
                assignedUsers,
                assignedGroups,
                assignedRoles,
                name,
                parentNode,
                description,
                proposalNote)));

    result.setAspects(aspects);

    return result;
  }

  @VisibleForTesting
  static ActionRequestSnapshot createCreateGlossaryTermProposalActionRequest(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description,
      @Nullable final String proposalNote) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(
        ActionRequestAspect.create(
            createCreateGlossaryTermActionRequestInfo(
                actorUrn,
                assignedUsers,
                assignedGroups,
                assignedRoles,
                name,
                parentNode,
                description,
                proposalNote)));

    result.setAspects(aspects);

    return result;
  }

  @VisibleForTesting
  static ActionRequestSnapshot createUpdateDescriptionProposalActionRequest(
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn resourceUrn,
      final String subResourceType,
      final String subResource,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String description,
      @Nullable final String proposalNote) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(
        ActionRequestAspect.create(
            createUpdateDescriptionActionRequestInfo(
                actorUrn,
                resourceUrn,
                subResourceType,
                subResource,
                assignedUsers,
                assignedGroups,
                assignedRoles,
                description,
                proposalNote)));

    result.setAspects(aspects);

    return result;
  }

  private ActionRequestSnapshot createDataContractActionRequest(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final Urn entityUrn,
      @Nonnull final DataContractProposalOperationType opType,
      @Nullable final List<FreshnessContract> freshness,
      @Nullable final List<SchemaContract> schema,
      @Nullable final List<DataQualityContract> quality,
      @Nullable final String description) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(
        ActionRequestAspect.create(
            createDataContractActionRequestInfo(
                actorUrn,
                assignedUsers,
                assignedGroups,
                entityUrn,
                opType,
                freshness,
                schema,
                quality,
                description)));
    result.setAspects(aspects);

    return result;
  }

  private static ActionRequestInfo createCreateGlossaryNodeActionRequestInfo(
      final Urn creator,
      final List<Urn> assignedUsers,
      final List<Urn> assignedGroups,
      final List<Urn> assignedRoles,
      final String name,
      final Optional<Urn> parentNode,
      final String description,
      @Nullable final String proposalNote) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(CREATE_GLOSSARY_NODE_ACTION_REQUEST_TYPE);
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setAssignedRoles(new UrnArray(assignedRoles));
    info.setResourceType(GLOSSARY_NODE_ENTITY_NAME);
    info.setParams(createCreateGlossaryNodeActionRequestParams(name, parentNode, description));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(creator);

    if (proposalNote != null) {
      info.setDescription(proposalNote);
    }

    return info;
  }

  private static ActionRequestInfo createCreateGlossaryTermActionRequestInfo(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description,
      @Nullable final String proposalNote) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(CREATE_GLOSSARY_TERM_ACTION_REQUEST_TYPE);
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setAssignedRoles(new UrnArray(assignedRoles));
    info.setResourceType(GLOSSARY_TERM_ENTITY_NAME);
    info.setParams(createCreateGlossaryTermActionRequestParams(name, parentNode, description));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(actorUrn);

    if (proposalNote != null) {
      info.setDescription(proposalNote);
    }

    return info;
  }

  private static ActionRequestInfo createUpdateDescriptionActionRequestInfo(
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn resourceUrn,
      final String subResourceType,
      final String subResource,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String description,
      @Nullable final String proposalNote) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(UPDATE_DESCRIPTION_ACTION_REQUEST_TYPE);
    info.setResource(resourceUrn.toString());
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setAssignedRoles(new UrnArray(assignedRoles));
    info.setResourceType(resourceUrn.getEntityType());
    if (subResourceType != null || subResource != null) {
      if (subResourceType == null || subResource == null) {
        throw new RuntimeException(
            String.format(
                "Only one of subResourceType and subResource specified. (subResourceType: %s, subResource: %s)",
                subResourceType, subResource));
      }
      info.setSubResourceType(subResourceType);
      info.setSubResource(subResource);
    }

    info.setParams(createUpdateDescriptionActionRequestParams(description));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(actorUrn);

    if (proposalNote != null) {
      info.setDescription(proposalNote);
    }

    return info;
  }

  private ActionRequestInfo createDataContractActionRequestInfo(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final Urn entityUrn,
      @Nonnull final DataContractProposalOperationType opType,
      @Nullable final List<FreshnessContract> freshness,
      @Nullable final List<SchemaContract> schema,
      @Nullable final List<DataQualityContract> quality,
      @Nullable final String description) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(DATA_CONTRACT_REQUEST_TYPE);
    info.setResource(entityUrn.toString());
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setResourceType(entityUrn.getEntityType());
    info.setParams(createDataContractParams(opType, freshness, schema, quality));
    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(actorUrn);
    if (description != null) {
      info.setDescription(description);
    }
    return info;
  }

  private static ActionRequestParams createCreateGlossaryNodeActionRequestParams(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description) {
    CreateGlossaryNodeProposal createGlossaryNodeProposal = new CreateGlossaryNodeProposal();
    createGlossaryNodeProposal.setName(name);
    parentNode.ifPresent(createGlossaryNodeProposal::setParentNode);
    if (description != null) {
      createGlossaryNodeProposal.setDescription(description);
    }

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setCreateGlossaryNodeProposal(createGlossaryNodeProposal);
    return actionRequestParams;
  }

  private static ActionRequestParams createCreateGlossaryTermActionRequestParams(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description) {
    CreateGlossaryTermProposal createGlossaryTermProposal = new CreateGlossaryTermProposal();
    createGlossaryTermProposal.setName(name);
    parentNode.ifPresent(createGlossaryTermProposal::setParentNode);
    if (description != null) {
      createGlossaryTermProposal.setDescription(description);
    }

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setCreateGlossaryTermProposal(createGlossaryTermProposal);
    return actionRequestParams;
  }

  private static ActionRequestParams createUpdateDescriptionActionRequestParams(
      @Nonnull final String description) {
    DescriptionProposal descriptionProposal = new DescriptionProposal();
    descriptionProposal.setDescription(description);

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setUpdateDescriptionProposal(descriptionProposal);
    return actionRequestParams;
  }

  private ActionRequestParams createDataContractParams(
      @Nonnull final DataContractProposalOperationType opType,
      @Nullable final List<FreshnessContract> freshness,
      @Nullable final List<SchemaContract> schema,
      @Nullable final List<DataQualityContract> quality) {
    final ActionRequestParams result = new ActionRequestParams();
    final DataContractProposal params = new DataContractProposal();
    params.setType(opType);
    if (freshness != null) {
      params.setFreshness(new FreshnessContractArray(freshness));
    }
    if (schema != null) {
      params.setSchema(new SchemaContractArray(schema));
    }
    if (quality != null) {
      params.setDataQuality(new DataQualityContractArray(quality));
    }
    result.setDataContractProposal(params);
    return result;
  }

  void createGlossaryNodeEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      @Nonnull final Optional<String> description)
      throws Exception {
    final GlossaryNodeKey key = new GlossaryNodeKey();

    final String id = UUID.randomUUID().toString();
    key.setName(id);

    if (this.entityClient.exists(
        opContext,
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.GLOSSARY_NODE_ENTITY_NAME))) {
      throw new IllegalArgumentException("This Glossary Node already exists!");
    }

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.GLOSSARY_NODE_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(mapGlossaryNodeInfo(name, parentNode, description)));
    proposal.setChangeType(ChangeType.UPSERT);

    this.entityClient.ingestProposal(opContext, proposal);
  }

  void createGlossaryTermEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      @Nonnull final Optional<String> description)
      throws Exception {
    final GlossaryTermKey key = new GlossaryTermKey();

    final String id = UUID.randomUUID().toString();
    key.setName(id);

    if (this.entityClient.exists(
        opContext,
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.GLOSSARY_TERM_ENTITY_NAME))) {
      throw new IllegalArgumentException("This Glossary Term already exists!");
    }

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.GLOSSARY_TERM_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(mapGlossaryTermInfo(name, parentNode, description)));
    proposal.setChangeType(ChangeType.UPSERT);

    this.entityClient.ingestProposal(opContext, proposal);
  }

  void updateGlossaryNodeDescription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description)
      throws Exception {
    GlossaryNodeInfo glossaryNodeInfo =
        (GlossaryNodeInfo)
            this.entityService.getLatestAspect(
                opContext, resourceUrn, GLOSSARY_NODE_INFO_ASPECT_NAME);
    Objects.requireNonNull(glossaryNodeInfo, "glossaryNodeInfo cannot be null");

    final MetadataChangeProposal proposal =
        DescriptionUtils.createGlossaryNodeDescriptionChangeProposal(
            glossaryNodeInfo, resourceUrn, description);
    this.entityClient.ingestProposal(opContext, proposal);
  }

  void updateGlossaryTermDescription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description)
      throws Exception {
    GlossaryTermInfo glossaryTermInfo =
        (GlossaryTermInfo)
            this.entityService.getLatestAspect(
                opContext, resourceUrn, GLOSSARY_TERM_INFO_ASPECT_NAME);

    final MetadataChangeProposal proposal =
        DescriptionUtils.createGlossaryTermDescriptionChangeProposal(
            glossaryTermInfo, resourceUrn, description);
    this.entityClient.ingestProposal(opContext, proposal);
  }

  void updateDatasetDescription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description)
      throws Exception {
    EditableDatasetProperties editableDatasetProperties =
        (EditableDatasetProperties)
            this.entityService.getLatestAspect(
                opContext, resourceUrn, EDITABLE_DATASET_PROPERTIES_ASPECT_NAME);
    if (editableDatasetProperties == null) {
      editableDatasetProperties = new EditableDatasetProperties();
    }

    final MetadataChangeProposal proposal =
        DescriptionUtils.createDatasetDescriptionChangeProposal(
            editableDatasetProperties,
            resourceUrn,
            description,
            opContext.getSessionAuthentication().getActor());
    this.entityClient.ingestProposal(opContext, proposal);
  }

  void updateSchemaFieldDescription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String fieldPath,
      @Nonnull final String description)
      throws Exception {
    EditableSchemaMetadata editableSchemaMetadata =
        (EditableSchemaMetadata)
            this.entityService.getLatestAspect(
                opContext, resourceUrn, EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    if (editableSchemaMetadata == null) {
      editableSchemaMetadata = new EditableSchemaMetadata();
    }

    final MetadataChangeProposal proposal =
        DescriptionUtils.createSchemaFieldDescriptionChangeProposal(
            editableSchemaMetadata,
            resourceUrn,
            fieldPath,
            description,
            opContext.getSessionAuthentication().getActor());
    this.entityClient.ingestProposal(opContext, proposal);
  }

  GlossaryNodeInfo mapGlossaryNodeInfo(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      @Nonnull final Optional<String> description)
      throws Exception {
    final GlossaryNodeInfo result = new GlossaryNodeInfo();
    result.setName(name);
    result.setDefinition("");
    if (parentNode.isPresent()) {
      final GlossaryNodeUrn nodeUrn = GlossaryNodeUrn.createFromString(parentNode.get().toString());
      result.setParentNode(nodeUrn);
    }
    description.ifPresent(result::setDefinition);

    return result;
  }

  GlossaryTermInfo mapGlossaryTermInfo(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      @Nonnull final Optional<String> description)
      throws Exception {
    final GlossaryTermInfo result = new GlossaryTermInfo();
    result.setName(name);
    result.setDefinition("");
    result.setTermSource("INTERNAL");
    if (parentNode.isPresent()) {
      final GlossaryNodeUrn nodeUrn = GlossaryNodeUrn.createFromString(parentNode.get().toString());
      result.setParentNode(nodeUrn);
    }
    description.ifPresent(result::setDefinition);

    return result;
  }

  private DataContractState getStateForContract(
      @Nonnull final Urn entityUrn, @Nonnull final DataContractProposal proposal) {
    // TOTAL HACK
    if (entityUrn.toString().contains("urn:li:dataPlatform:dbt")) {
      // Assertions must be provisioned for this contract.
      // This is a really bad way to do this. Ideally we know if the assertions are external or not.
      // We should check them before creating the contract.
      return DataContractState.PENDING;
    }
    return DataContractState.ACTIVE;
  }

  private void verifyAssertionsExist(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> assertionUrns) {
    final Set<Urn> existing = this.entityService.exists(opContext, assertionUrns);
    for (final Urn assertionUrn : assertionUrns) {
      if (!existing.contains(assertionUrn)) {
        throw new RuntimeException(String.format("Assertion %s does not exist", assertionUrn));
      }
    }
  }
}
