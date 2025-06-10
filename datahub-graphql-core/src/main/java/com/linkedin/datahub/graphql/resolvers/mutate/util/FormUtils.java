package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;
import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.convertFilterToTestConditions;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.CreateDynamicFormAssignmentInput;
import com.linkedin.datahub.graphql.generated.CreateFormInput;
import com.linkedin.datahub.graphql.generated.CreatePromptInput;
import com.linkedin.datahub.graphql.generated.DomainParamsInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FormActorAssignmentInput;
import com.linkedin.datahub.graphql.generated.FormAssetAssignment;
import com.linkedin.datahub.graphql.generated.FormFilter;
import com.linkedin.datahub.graphql.generated.FormNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.FormSettingsInput;
import com.linkedin.datahub.graphql.generated.GlossaryTermsParamsInput;
import com.linkedin.datahub.graphql.generated.OwnershipParamsInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyParamsInput;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.form.DomainParams;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormNotificationSettings;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.FormSettings;
import com.linkedin.form.FormState;
import com.linkedin.form.FormStatus;
import com.linkedin.form.FormType;
import com.linkedin.form.GlossaryTermsParams;
import com.linkedin.form.OwnershipParams;
import com.linkedin.form.PromptCardinality;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.test.MetadataTestClient;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class FormUtils {

  private static final String COMPLETED_FORMS = "completedForms";
  private static final String INCOMPLETE_FORMS = "incompleteForms";
  private static final String VERIFIED_FORMS = "verifiedForms";
  private static final String COMPLETED_FORMS_COMPLETED_PROMPT_IDS =
      "completedFormsCompletedPromptIds";
  private static final String INCOMPLETE_FORMS_COMPLETED_PROMPT_IDS =
      "incompleteFormsCompletedPromptIds";

  private FormUtils() {}

  public static PrimitivePropertyValueArray getStructuredPropertyValuesFromInput(
      @Nonnull final SubmitFormPromptInput input) {
    final PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();

    input
        .getStructuredPropertyParams()
        .getValues()
        .forEach(value -> values.add(StructuredPropertyUtils.mapPropertyValueInput(value)));

    return values;
  }

  /** Map a GraphQL CreateDynamicFormAssignmentInput to the GMS DynamicFormAssignment aspect */
  @Nonnull
  public static DynamicFormAssignment mapDynamicFormAssignment(
      @Nonnull final CreateDynamicFormAssignmentInput input) {
    Objects.requireNonNull(input, "input must not be null");

    final DynamicFormAssignment result = new DynamicFormAssignment();
    final Filter filter =
        new Filter()
            .setOr(ResolverUtils.buildConjunctiveCriterionArrayWithOr(input.getOrFilters()));
    result.setFilter(filter);
    return result;
  }

  /**
   * Creates the filter based on the FormFilter input from the graphql endpoint to filter entities
   * by Form related fields.
   */
  public static Filter buildFormFilter(
      @Nonnull final FormFilter formFilter,
      @Nonnull final FormInfo formInfo,
      @Nonnull final List<Urn> groupsForUser)
      throws Exception {
    final CriterionArray andArray = new CriterionArray();
    final String formUrn = formFilter.getFormUrn();

    Criterion assignedActorCriterion =
        getAssignedActorCriterion(formFilter, formInfo, groupsForUser);
    if (assignedActorCriterion != null) {
      andArray.add(assignedActorCriterion);
    }

    Filter promptFilter = null;
    if (formFilter.getPromptId() != null && formFilter.getIsPromptComplete() != null) {
      promptFilter = generatePromptFilter(formFilter);
    }

    if (formFilter.getIsFormVerified() != null) {
      andArray.add(generateVerifiedCriterion(formFilter, formUrn));
    }

    // if isFormComplete is not specified, get either completed or incomplete forms
    Filter completeOrIncompleteFilter = null;
    if (formFilter.getIsFormComplete() == null) {
      completeOrIncompleteFilter = generateCompleteOrIncompleteFilter(formUrn);
    } else {
      andArray.add(
          buildFormCriterion(
              formUrn, formFilter.getIsFormComplete() ? COMPLETED_FORMS : INCOMPLETE_FORMS));
    }

    Filter finalFilter =
        new Filter()
            .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(andArray)));
    if (promptFilter != null) {
      finalFilter = FilterUtils.combineFilters(finalFilter, promptFilter);
    }
    if (completeOrIncompleteFilter != null) {
      finalFilter = FilterUtils.combineFilters(finalFilter, completeOrIncompleteFilter);
    }

    return finalFilter;
  }

  private static Filter generatePromptCompleteFilter(@Nonnull final FormFilter formFilter) {
    final CriterionArray completedFormsPromptIdsAndArray = new CriterionArray();
    final CriterionArray incompleteFormsPromptIdsAndArray = new CriterionArray();
    // if the prompt is complete, it will be in one of these fields
    completedFormsPromptIdsAndArray.add(
        buildFormCriterion(formFilter.getPromptId(), COMPLETED_FORMS_COMPLETED_PROMPT_IDS));
    incompleteFormsPromptIdsAndArray.add(
        buildFormCriterion(formFilter.getPromptId(), INCOMPLETE_FORMS_COMPLETED_PROMPT_IDS));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(completedFormsPromptIdsAndArray),
                new ConjunctiveCriterion().setAnd(incompleteFormsPromptIdsAndArray)));
  }

  private static Filter generatePromptIncompleteFilter(@Nonnull final FormFilter formFilter) {
    final CriterionArray andArray = new CriterionArray();
    // if it's not complete, it should not be in any of these fields
    andArray.add(
        buildFormCriterion(formFilter.getPromptId(), COMPLETED_FORMS_COMPLETED_PROMPT_IDS, true));
    andArray.add(
        buildFormCriterion(formFilter.getPromptId(), INCOMPLETE_FORMS_COMPLETED_PROMPT_IDS, true));
    return new Filter()
        .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(andArray)));
  }

  private static Filter generatePromptFilter(@Nonnull final FormFilter formFilter) {
    if (formFilter.getIsPromptComplete()) {
      return generatePromptCompleteFilter(formFilter);
    }
    return generatePromptIncompleteFilter(formFilter);
  }

  private static Criterion generateVerifiedCriterion(
      @Nonnull final FormFilter formFilter, @Nonnull final String formUrn) {
    if (formFilter.getIsFormVerified()) {
      return buildFormCriterion(formUrn, VERIFIED_FORMS);
    }
    return buildFormCriterion(formUrn, VERIFIED_FORMS, true);
  }

  /**
   * Creates a Filter where the provided formUrn is either in completedForms or incompleteForms for
   * an entity
   */
  private static Filter generateCompleteOrIncompleteFilter(@Nonnull final String formUrn)
      throws Exception {
    final CriterionArray completedFormsAndArray = new CriterionArray();
    final CriterionArray incompleteFormsAndArray = new CriterionArray();
    completedFormsAndArray.add(buildFormCriterion(formUrn, COMPLETED_FORMS));
    incompleteFormsAndArray.add(buildFormCriterion(formUrn, INCOMPLETE_FORMS));
    // need this to be an OR not two ANDs
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(completedFormsAndArray),
                new ConjunctiveCriterion().setAnd(incompleteFormsAndArray)));
  }

  private static Criterion buildFormCriterion(
      @Nonnull final String formUrn, @Nonnull final String field) {
    return buildFormCriterion(formUrn, field, false);
  }

  private static Criterion buildFormCriterion(
      @Nonnull final String formUrn, @Nonnull final String field, final boolean negated) {
    return buildCriterion(field, Condition.EQUAL, negated, formUrn);
  }

  /**
   * Creates an owners filter if the form is based on owners for the given assigned actor.
   * Otherwise, check if the given actor is explicitly assigned on the form, otherwise throw an
   * error since this actor can't be assigned to fill out this form for any entities.
   */
  @Nullable
  private static Criterion getAssignedActorCriterion(
      @Nonnull final FormFilter formFilter,
      @Nonnull final FormInfo formInfo,
      @Nonnull final List<Urn> groupsForUser) {
    final String assignedActor = formFilter.getAssignedActor();
    if (assignedActor == null) {
      return null;
    }

    final Urn actorUrn = UrnUtils.getUrn(assignedActor);
    if (isActorExplicitlyAssigned(actorUrn, formInfo, groupsForUser)) {
      return null;
    }
    if (formInfo.getActors().isOwners()) {
      return com.linkedin.metadata.utils.FormUtils.getFormOwnershipCriterion(
          actorUrn, groupsForUser);
    }
    throw new RuntimeException("This form is not assigned to the provided assignedActor");
  }

  private static boolean isActorExplicitlyAssigned(
      @Nonnull final Urn actorUrn,
      @Nonnull final FormInfo formInfo,
      @Nonnull final List<Urn> groupsForUser) {
    return (formInfo.getActors().getUsers() != null
            && formInfo.getActors().getUsers().stream().anyMatch(user -> user.equals(actorUrn)))
        || (formInfo.getActors().getGroups() != null
            && formInfo.getActors().getGroups().stream()
                .anyMatch(
                    group ->
                        groupsForUser.stream().anyMatch(userGroup -> userGroup.equals(group))));
  }

  @Nonnull
  public static DynamicFormAssignment updateDynamicFormAssignment(
      @Nonnull OperationContext context,
      @Nullable final EntityResponse response,
      @Nonnull final FormAssetAssignment formAssetAssignmentInput) {
    Objects.requireNonNull(formAssetAssignmentInput, "formAssetAssignmentInput must not be null");

    final DynamicFormAssignment dynamicFormAssignment =
        response != null
                && response.getAspects().containsKey(Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME)
            ? new DynamicFormAssignment(
                response
                    .getAspects()
                    .get(Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME)
                    .getValue()
                    .data())
            : new DynamicFormAssignment();

    if (formAssetAssignmentInput.getOrFilters() != null) {
      final Filter filter =
          new Filter()
              .setOr(
                  ResolverUtils.buildConjunctiveCriterionArrayWithOr(
                      formAssetAssignmentInput.getOrFilters()));
      dynamicFormAssignment.setFilter(filter);
    }
    if (formAssetAssignmentInput.getJson() != null) {
      dynamicFormAssignment.setJson(formAssetAssignmentInput.getJson());
    }

    return dynamicFormAssignment;
  }

  @Nonnull
  public static FormSettings updateFormSettings(
      @Nonnull OperationContext context,
      @Nullable final EntityResponse response,
      @Nonnull final FormSettingsInput formSettingsInput) {
    Objects.requireNonNull(formSettingsInput, "formSettingsInput must not be null");

    final FormSettings formSettings =
        response != null && response.getAspects().containsKey(Constants.FORM_SETTINGS_ASPECT_NAME)
            ? new FormSettings(
                response.getAspects().get(Constants.FORM_SETTINGS_ASPECT_NAME).getValue().data())
            : new FormSettings();

    FormNotificationSettings formNotificationSettings =
        getFormNotificationSettings(formSettings, formSettingsInput);
    formSettings.setNotificationSettings(formNotificationSettings);

    return formSettings;
  }

  private static FormNotificationSettings getFormNotificationSettings(
      final FormSettings formSettings, FormSettingsInput formSettingsInput) {
    FormNotificationSettings formNotificationSettings =
        formSettings.hasNotificationSettings()
            ? formSettings.getNotificationSettings()
            : new FormNotificationSettings();
    if (formSettingsInput.getNotificationSettings() != null) {
      FormNotificationSettingsInput formNotificationSettingsInput =
          formSettingsInput.getNotificationSettings();

      if (formNotificationSettingsInput.getNotifyAssigneesOnPublish() != null) {
        formNotificationSettings.setNotifyAssigneesOnPublish(
            formNotificationSettingsInput.getNotifyAssigneesOnPublish());
      }
    }
    return formNotificationSettings;
  }

  @Nonnull
  public static FormInfo mapFormInfo(
      @Nonnull OperationContext context, @Nonnull final CreateFormInput input) {
    Objects.requireNonNull(input, "input must not be null");

    final FormInfo result = new FormInfo();
    result.setName(input.getName());
    if (input.getDescription() != null) {
      result.setDescription(input.getDescription());
    }
    if (input.getType() != null) {
      result.setType(FormType.valueOf(input.getType().toString()));
    }
    if (input.getPrompts() != null) {
      result.setPrompts(mapPrompts(input.getPrompts()));
    }
    if (input.getActors() != null) {
      result.setActors(mapFormActorAssignment(input.getActors()));
    }
    // always set status when creating a form
    result.setStatus(createFormStatus(context, input));
    result.setCreated(context.getAuditStamp());
    result.setLastModified(context.getAuditStamp());

    return result;
  }

  private static FormStatus createFormStatus(
      @Nonnull OperationContext context, @Nonnull final CreateFormInput input) {
    FormStatus formStatus = new FormStatus();
    if (input.getState() != null) {
      formStatus.setState(FormState.valueOf(input.getState().toString()));
    } else {
      // default set state as draft when creating a form
      formStatus.setState(FormState.DRAFT);
    }
    formStatus.setLastModified(context.getAuditStamp());
    return formStatus;
  }

  @Nonnull
  public static FormPromptArray mapPrompts(@Nonnull final List<CreatePromptInput> promptInputs) {
    Objects.requireNonNull(promptInputs, "promptInputs must not be null");

    final FormPromptArray result = new FormPromptArray();
    promptInputs.forEach(
        promptInput -> {
          result.add(mapPrompt(promptInput));
        });
    return result;
  }

  @Nonnull
  public static FormPrompt mapPrompt(@Nonnull final CreatePromptInput promptInput) {
    Objects.requireNonNull(promptInput, "promptInput must not be null");

    final FormPrompt result = new FormPrompt();
    String promptId =
        promptInput.getId() != null ? promptInput.getId() : UUID.randomUUID().toString();
    result.setId(promptId);
    result.setTitle(promptInput.getTitle());
    if (promptInput.getDescription() != null) {
      result.setDescription(promptInput.getDescription());
    }
    if (promptInput.getType() != null) {
      result.setType(FormPromptType.valueOf(promptInput.getType().toString()));
    }
    if (promptInput.getStructuredPropertyParams() != null) {
      result.setStructuredPropertyParams(
          mapStructuredPropertyParams(promptInput.getStructuredPropertyParams()));
    }
    if (promptInput.getOwnershipParams() != null) {
      result.setOwnershipParams(mapOwnershipParams(promptInput.getOwnershipParams()));
    }
    if (promptInput.getGlossaryTermsParams() != null) {
      result.setGlossaryTermsParams(mapGlossaryTermsParams(promptInput.getGlossaryTermsParams()));
    }
    if (promptInput.getDomainParams() != null) {
      result.setDomainParams(mapDomainParams(promptInput.getDomainParams()));
    }
    if (promptInput.getRequired() != null) {
      result.setRequired(promptInput.getRequired());
    }

    return result;
  }

  @Nonnull
  public static StructuredPropertyParams mapStructuredPropertyParams(
      @Nonnull final StructuredPropertyParamsInput paramsInput) {
    Objects.requireNonNull(paramsInput, "paramsInput must not be null");

    final StructuredPropertyParams result = new StructuredPropertyParams();
    result.setUrn(UrnUtils.getUrn(paramsInput.getUrn()));
    return result;
  }

  @Nonnull
  public static OwnershipParams mapOwnershipParams(
      @Nonnull final OwnershipParamsInput paramsInput) {
    Objects.requireNonNull(paramsInput, "paramsInput must not be null");

    final OwnershipParams result = new OwnershipParams();
    result.setCardinality(
        paramsInput.getCardinality() != null
            ? PromptCardinality.valueOf(paramsInput.getCardinality().toString())
            : PromptCardinality.MULTIPLE);
    if (paramsInput.getAllowedOwners() != null) {
      UrnArray allowedOwners = new UrnArray();
      paramsInput
          .getAllowedOwners()
          .forEach(termUrn -> allowedOwners.add(UrnUtils.getUrn(termUrn)));
      result.setAllowedOwners(allowedOwners);
    }
    if (paramsInput.getAllowedOwnershipTypes() != null) {
      UrnArray allowedOwnershipTypes = new UrnArray();
      paramsInput
          .getAllowedOwnershipTypes()
          .forEach(termGroupUrn -> allowedOwnershipTypes.add(UrnUtils.getUrn(termGroupUrn)));
      result.setAllowedOwnershipTypes(allowedOwnershipTypes);
    }
    return result;
  }

  @Nonnull
  public static GlossaryTermsParams mapGlossaryTermsParams(
      @Nonnull final GlossaryTermsParamsInput paramsInput) {
    Objects.requireNonNull(paramsInput, "paramsInput must not be null");

    final GlossaryTermsParams result = new GlossaryTermsParams();
    result.setCardinality(
        paramsInput.getCardinality() != null
            ? PromptCardinality.valueOf(paramsInput.getCardinality().toString())
            : PromptCardinality.MULTIPLE);
    if (paramsInput.getAllowedTerms() != null) {
      UrnArray allowedTerms = new UrnArray();
      paramsInput.getAllowedTerms().forEach(termUrn -> allowedTerms.add(UrnUtils.getUrn(termUrn)));
      result.setAllowedTerms(allowedTerms);
    }
    if (paramsInput.getAllowedTermGroups() != null) {
      UrnArray allowedTermGroups = new UrnArray();
      paramsInput
          .getAllowedTermGroups()
          .forEach(termGroupUrn -> allowedTermGroups.add(UrnUtils.getUrn(termGroupUrn)));
      result.setAllowedTermGroups(allowedTermGroups);
    }
    return result;
  }

  @Nonnull
  public static DomainParams mapDomainParams(@Nonnull final DomainParamsInput paramsInput) {
    Objects.requireNonNull(paramsInput, "paramsInput must not be null");

    final DomainParams result = new DomainParams();
    if (paramsInput.getAllowedDomains() != null) {
      UrnArray allowedDomains = new UrnArray();
      paramsInput
          .getAllowedDomains()
          .forEach(termUrn -> allowedDomains.add(UrnUtils.getUrn(termUrn)));
      result.setAllowedDomains(allowedDomains);
    }
    return result;
  }

  @Nonnull
  public static FormActorAssignment mapFormActorAssignment(
      @Nonnull final FormActorAssignmentInput input) {
    Objects.requireNonNull(input, "input must not be null");

    final FormActorAssignment result = new FormActorAssignment();
    if (input.getOwners() != null) {
      result.setOwners(input.getOwners());
    }
    if (input.getUsers() != null) {
      UrnArray userUrns = new UrnArray();
      input.getUsers().forEach(user -> userUrns.add(UrnUtils.getUrn(user)));
      result.setUsers(userUrns);
    }
    if (input.getGroups() != null) {
      UrnArray groupUrns = new UrnArray();
      input.getGroups().forEach(group -> groupUrns.add(UrnUtils.getUrn(group)));
      result.setGroups(groupUrns);
    }

    return result;
  }

  @Nonnull
  public static List<FormPrompt> mapPromptsToAdd(
      @Nonnull final List<CreatePromptInput> promptsToAdd) {
    return promptsToAdd.stream().map(FormUtils::mapPrompt).collect(Collectors.toList());
  }

  /*
   * Takes a list of orFilters and a formFilter and combines them to a single filter
   */
  public static Filter combineFormFilterAndOrFilters(
      QueryContext context,
      FormService formService,
      List<AndFilterInput> orFilters,
      FormFilter formFilter) {
    final Filter inputFilter = ResolverUtils.buildFilter(null, orFilters);
    final Filter finalFormFilter =
        SearchUtils.getFormFilter(context.getOperationContext(), formFilter, formService);
    return finalFormFilter != null
        ? FilterUtils.combineFilters(inputFilter, finalFormFilter)
        : inputFilter;
  }

  /*
   * Async evaluate the test
   */
  public static void runTest(
      QueryContext context, MetadataTestClient metadataTestClient, String taskUrn) {
    GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            metadataTestClient.evaluateSingleTest(
                UrnUtils.getUrn(taskUrn), true, context.getAuthentication());
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to evaluate bulk form response task with urn %s", taskUrn),
                e);
          }
          return null;
        },
        FormUtils.class.getSimpleName(),
        "runTest");
  }

  public static ObjectNode buildBulkFormDefinitionOnNode(
      QueryContext context,
      List<EntityType> types,
      Filter filter,
      @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs) {
    ObjectMapper objectMapper = context.getOperationContext().getObjectMapper();
    ObjectNode definitionNode = objectMapper.createObjectNode();
    definitionNode.putArray("rules");

    // create on clause with conditions
    ObjectNode onNode = definitionNode.putObject("on");
    ArrayNode typesArray = onNode.putArray("types");
    getEntityNames(types).forEach(typesArray::add);

    ObjectNode conditionsNode = onNode.putObject("conditions");
    ArrayNode orConditions = conditionsNode.putArray("or");
    ObjectNode filterNode = orConditions.addObject();
    ArrayNode orArray = filterNode.putArray("or");
    orArray.addAll(convertFilterToTestConditions(filter, searchableFieldsToPathSpecs));

    return definitionNode;
  }
}
