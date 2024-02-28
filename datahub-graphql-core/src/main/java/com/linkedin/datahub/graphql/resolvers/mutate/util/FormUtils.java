package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.CreateDynamicFormAssignmentInput;
import com.linkedin.datahub.graphql.generated.FormFilter;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
        .forEach(
            value -> {
              if (value.getStringValue() != null) {
                values.add(PrimitivePropertyValue.create(value.getStringValue()));
              } else if (value.getNumberValue() != null) {
                values.add(PrimitivePropertyValue.create(value.getNumberValue().doubleValue()));
              }
            });

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
      @Nonnull final FormFilter formFilter, @Nonnull final FormInfo formInfo) throws Exception {
    final CriterionArray andArray = new CriterionArray();
    final String formUrn = formFilter.getFormUrn();

    Criterion assignedActorCriterion = getAssignedActorCriterion(formFilter, formInfo);
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
      finalFilter = SearchUtils.combineFilters(finalFilter, promptFilter);
    }
    if (completeOrIncompleteFilter != null) {
      finalFilter = SearchUtils.combineFilters(finalFilter, completeOrIncompleteFilter);
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
    return new Criterion()
        .setField(field)
        .setValue(formUrn)
        .setCondition(Condition.EQUAL)
        .setNegated(negated);
  }

  /**
   * Creates an owners filter if the form is based on owners for the given assigned actor.
   * Otherwise, check if the given actor is explicitly assigned on the form, otherwise throw an
   * error since this actor can't be assigned to fill out this form for any entities.
   */
  @Nullable
  private static Criterion getAssignedActorCriterion(
      @Nonnull final FormFilter formFilter, @Nonnull final FormInfo formInfo) {
    final String assignedActor = formFilter.getAssignedActor();
    if (assignedActor == null) {
      return null;
    }

    final Urn actorUrn = UrnUtils.getUrn(assignedActor);
    if (isActorExplicitlyAssigned(actorUrn, formInfo)) {
      return null;
    }
    if (formInfo.getActors().isOwners()) {
      return new Criterion().setField(OWNERS).setValue(assignedActor).setCondition(Condition.EQUAL);
    }
    throw new RuntimeException("This form is not assigned to the provided assignedActor");
  }

  private static boolean isActorExplicitlyAssigned(
      @Nonnull final Urn actorUrn, @Nonnull final FormInfo formInfo) {
    return (formInfo.getActors().getUsers() != null
            && formInfo.getActors().getUsers().stream().anyMatch(user -> user.equals(actorUrn)))
        || (formInfo.getActors().getGroups() != null
            && formInfo.getActors().getGroups().stream().anyMatch(group -> group.equals(actorUrn)));
  }
}
