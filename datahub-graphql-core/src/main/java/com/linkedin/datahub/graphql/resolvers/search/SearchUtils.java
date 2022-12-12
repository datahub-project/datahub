package com.linkedin.datahub.graphql.resolvers.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.plexus.util.CollectionUtils;


@Slf4j
public class SearchUtils {
  private SearchUtils() {
  }

  /**
   * Entities that are searched by default in Search Across Entities
   */
  public static final List<EntityType> SEARCHABLE_ENTITY_TYPES =
      ImmutableList.of(
          EntityType.DATASET,
          EntityType.DASHBOARD,
          EntityType.CHART,
          EntityType.MLMODEL,
          EntityType.MLMODEL_GROUP,
          EntityType.MLFEATURE_TABLE,
          EntityType.MLFEATURE,
          EntityType.MLPRIMARY_KEY,
          EntityType.DATA_FLOW,
          EntityType.DATA_JOB,
          EntityType.GLOSSARY_TERM,
          EntityType.GLOSSARY_NODE,
          EntityType.TAG,
          EntityType.CORP_USER,
          EntityType.CORP_GROUP,
          EntityType.CONTAINER,
          EntityType.DOMAIN,
          EntityType.NOTEBOOK);

  /**
   * Entities that are part of autocomplete by default in Auto Complete Across Entities
   */
  public static final List<EntityType> AUTO_COMPLETE_ENTITY_TYPES =
      ImmutableList.of(
          EntityType.DATASET,
          EntityType.DASHBOARD,
          EntityType.CHART,
          EntityType.MLMODEL,
          EntityType.MLMODEL_GROUP,
          EntityType.MLFEATURE_TABLE,
          EntityType.DATA_FLOW,
          EntityType.DATA_JOB,
          EntityType.GLOSSARY_TERM,
          EntityType.TAG,
          EntityType.CORP_USER,
          EntityType.CORP_GROUP,
          EntityType.NOTEBOOK);


  /**
   * Combines two {@link Filter} instances in a conjunction and returns a new instance of {@link Filter}
   * in disjunctive normal form.
   *
   * @param baseFilter the filter to apply the view to
   * @param viewFilter the view filter, null if it doesn't exist
   *
   * @return a new instance of {@link Filter} representing the applied view.
   */
  @Nonnull
  public static Filter combineFilters(@Nullable final Filter baseFilter, @Nonnull final Filter viewFilter) {
    final Filter finalBaseFilter = baseFilter == null
        ? new Filter().setOr(new ConjunctiveCriterionArray(Collections.emptyList()))
        : baseFilter;

    // Join the filter conditions in Disjunctive Normal Form.
    return combineFiltersInConjunction(finalBaseFilter, viewFilter);
  }

  /**
   * Returns the intersection of two sets of entity types. (Really just string lists).
   * If either is empty, consider the entity types list to mean "all" (take the other set).
   *
   * @param baseEntityTypes the entity types to apply the view to
   * @param viewEntityTypes the view info, null if it doesn't exist
   *
   * @return the intersection of the two input sets
   */
  @Nonnull
  public static List<String> intersectEntityTypes(@Nonnull final List<String> baseEntityTypes, @Nonnull final List<String> viewEntityTypes) {
    if (baseEntityTypes.isEmpty()) {
      return viewEntityTypes;
    }
    if (viewEntityTypes.isEmpty()) {
      return baseEntityTypes;
    }
    // Join the entity types in intersection.
    return new ArrayList<>(CollectionUtils.intersection(baseEntityTypes, viewEntityTypes));
  }

  /**
   * Joins two filters in conjunction by reducing to Disjunctive Normal Form.
   *
   * @param filter1 the first filter in the pair
   * @param filter2 the second filter in the pair
   *
   * This method supports either Filter format, where the "or" field is used, instead
   * of criteria. If the criteria filter is used, then it will be converted into an "OR" before
   * returning the new filter.
   *
   * @return the result of joining the 2 filters in a conjunction (AND)
   *
   * How does it work? It basically cross-products the conjunctions inside of each Filter clause.
   *
   * Example Inputs:
   *        filter1 ->
   *                {
   *                  or: [
   *                    {
   *                      and: [
   *                        {
   *                          field: tags,
   *                          condition: EQUAL,
   *                          values: ["urn:li:tag:tag"]
   *                        }
   *                      ]
   *                    },
   *                    {
   *                      and: [
   *                        {
   *                          field: glossaryTerms,
   *                          condition: EQUAL,
   *                          values: ["urn:li:glossaryTerm:term"]
   *                        }
   *                      ]
   *                    }
   *                  ]
   *                }
   *        filter2 ->
   *                {
   *                  or: [
   *                    {
   *                      and: [
   *                        {
   *                          field: domain,
   *                          condition: EQUAL,
   *                          values: ["urn:li:domain:domain"]
   *                        },
   *                      ]
   *                    },
   *                    {
   *                      and: [
   *                        {
   *                          field: glossaryTerms,
   *                          condition: EQUAL,
   *                          values: ["urn:li:glossaryTerm:term2"]
   *                        }
   *                      ]
   *                    }
   *                  ]
   *                }
   *  Example Output:
   *                {
   *                  or: [
   *                    {
   *                      and: [
   *                        {
   *                          field: tags,
   *                          condition: EQUAL,
   *                          values: ["urn:li:tag:tag"]
   *                        },
   *                        {
   *                          field: domain,
   *                          condition: EQUAL,
   *                          values: ["urn:li:domain:domain"]
   *                        }
   *                      ]
   *                    },
   *                    {
   *                      and: [
   *                        {
   *                          field: tags,
   *                          condition: EQUAL,
   *                          values: ["urn:li:tag:tag"]
   *                        },
   *                        {
   *                          field: glossaryTerms,
   *                          condition: EQUAL,
   *                          values: ["urn:li:glosaryTerm:term2"]
   *                        }
   *                      ]
   *                    },
   *                    {
   *                      and: [
   *                        {
   *                          field: glossaryTerm,
   *                          condition: EQUAL,
   *                          values: ["urn:li:glossaryTerm:term"]
   *                        },
   *                        {
   *                          field: domain,
   *                          condition: EQUAL,
   *                          values: ["urn:li:domain:domain"]
   *                        }
   *                      ]
   *                    },
   *                    {
   *                      and: [
   *                        {
   *                          field: glossaryTerm,
   *                          condition: EQUAL,
   *                          values: ["urn:li:glossaryTerm:term"]
   *                        },
   *                        {
   *                          field: glossaryTerms,
   *                          condition: EQUAL,
   *                          values: ["urn:li:glosaryTerm:term2"]
   *                        }
   *                      ]
   *                    },
   *                  ]
   *                }
   */
  @Nonnull
  private static Filter combineFiltersInConjunction(@Nonnull final Filter filter1, @Nonnull final Filter filter2) {

    final Filter finalFilter1 = convertToV2Filter(filter1);
    final Filter finalFilter2 = convertToV2Filter(filter2);

    // If either filter is empty, simply return the other filter.
    if (!finalFilter1.hasOr() || finalFilter1.getOr().size() == 0) {
      return finalFilter2;
    }
    if (!finalFilter2.hasOr() || finalFilter2.getOr().size() == 0) {
      return finalFilter1;
    }

    // Iterate through the base filter, then cross-product with filter 2 conditions.
    final Filter result = new Filter();
    final List<ConjunctiveCriterion> newDisjunction = new ArrayList<>();
    for (ConjunctiveCriterion conjunction1 : finalFilter1.getOr()) {
      for (ConjunctiveCriterion conjunction2 : finalFilter2.getOr()) {
        final List<Criterion> joinedCriterion = new ArrayList<>(conjunction1.getAnd());
        joinedCriterion.addAll(conjunction2.getAnd());
        ConjunctiveCriterion newConjunction = new ConjunctiveCriterion().setAnd(new CriterionArray(joinedCriterion));
        newDisjunction.add(newConjunction);
      }
    }
    result.setOr(new ConjunctiveCriterionArray(newDisjunction));
    return result;
  }

  @Nonnull
  private static Filter convertToV2Filter(@Nonnull Filter filter) {
    if (filter.hasOr()) {
      return filter;
    } else if (filter.hasCriteria()) {
      // Convert criteria to an OR
      return new Filter()
          .setOr(new ConjunctiveCriterionArray(ImmutableList.of(
              new ConjunctiveCriterion()
                .setAnd(filter.getCriteria())
          )));
    }
    throw new IllegalArgumentException(
        String.format("Illegal filter provided! Neither 'or' nor 'criteria' fields were populated for filter %s", filter));
  }
}
