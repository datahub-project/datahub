package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.CreateDynamicFormAssignmentInput;
import com.linkedin.datahub.graphql.generated.CreateFormInput;
import com.linkedin.datahub.graphql.generated.CreatePromptInput;
import com.linkedin.datahub.graphql.generated.FormActorAssignmentInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyParamsInput;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.FormType;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.structured.PrimitivePropertyValueArray;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class FormUtils {

  private static final String COMPLETED_FORMS = "completedForms";
  private static final String INCOMPLETE_FORMS = "incompleteForms";
  private static final String VERIFIED_FORMS = "verifiedForms";
  private static final String OWNERS = "owners";
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

  private static boolean isActorExplicitlyAssigned(
      @Nonnull final Urn actorUrn, @Nonnull final FormInfo formInfo) {
    return (formInfo.getActors().getUsers() != null
            && formInfo.getActors().getUsers().stream().anyMatch(user -> user.equals(actorUrn)))
        || (formInfo.getActors().getGroups() != null
            && formInfo.getActors().getGroups().stream().anyMatch(group -> group.equals(actorUrn)));
  }

  @Nonnull
  public static FormInfo mapFormInfo(@Nonnull final CreateFormInput input) {
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

    return result;
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
}
