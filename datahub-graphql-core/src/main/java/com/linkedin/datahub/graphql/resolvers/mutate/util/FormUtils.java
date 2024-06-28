package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.CreateDynamicFormAssignmentInput;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
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
        .forEach(value -> values.add(StructuredPropertyUtils.mapPropertyValueInput(value)));

    return values;
  }

  /** Map a GraphQL CreateDynamicFormAssignmentInput to the GMS DynamicFormAssignment aspect */
  @Nonnull
  public static DynamicFormAssignment mapDynamicFormAssignment(
      @Nonnull final CreateDynamicFormAssignmentInput input,
      @Nullable AspectRetriever aspectRetriever) {
    Objects.requireNonNull(input, "input must not be null");

    final DynamicFormAssignment result = new DynamicFormAssignment();
    final Filter filter =
        new Filter()
            .setOr(
                ResolverUtils.buildConjunctiveCriterionArrayWithOr(
                    input.getOrFilters(), aspectRetriever));
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
    return new Criterion()
        .setField(field)
        .setValue(formUrn)
        .setCondition(Condition.EQUAL)
        .setNegated(negated);
  }

  private static boolean isActorExplicitlyAssigned(
      @Nonnull final Urn actorUrn, @Nonnull final FormInfo formInfo) {
    return (formInfo.getActors().getUsers() != null
            && formInfo.getActors().getUsers().stream().anyMatch(user -> user.equals(actorUrn)))
        || (formInfo.getActors().getGroups() != null
            && formInfo.getActors().getGroups().stream().anyMatch(group -> group.equals(actorUrn)));
  }
}
