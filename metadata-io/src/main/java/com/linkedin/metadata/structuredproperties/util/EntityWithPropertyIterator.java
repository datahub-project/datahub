package com.linkedin.metadata.structuredproperties.util;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX;
import static com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;

/** Fetches pages of entity urns which have a value for the given structured property definition */
@Builder
public class EntityWithPropertyIterator implements Iterator<ScrollResult> {
  @Nonnull private final Urn propertyUrn;
  @Nullable private final StructuredPropertyDefinition definition;
  @Nonnull private final SearchRetriever searchRetriever;
  private int count;
  @Builder.Default private String scrollId = null;
  @Builder.Default private boolean started = false;

  private List<String> getEntities() {
    if (definition != null && definition.getEntityTypes() != null) {
      return definition.getEntityTypes().stream()
          .map(StructuredPropertyUtils::getValueTypeId)
          .collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  private Filter getFilter() {
    Filter propertyFilter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    // Cannot rely on automatic field name since the definition is deleted
    final Criterion propertyExistsCriterion =
        buildExistsCriterion(
            STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX
                + StructuredPropertyUtils.toElasticsearchFieldName(propertyUrn, definition));

    andCriterion.add(propertyExistsCriterion);
    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    propertyFilter.setOr(disjunction);

    return propertyFilter;
  }

  @Override
  public boolean hasNext() {
    return !started || scrollId != null;
  }

  @Override
  public ScrollResult next() {
    started = true;
    ScrollResult result = searchRetriever.scroll(getEntities(), getFilter(), scrollId, count);
    scrollId = result.getScrollId();
    return result;
  }
}
