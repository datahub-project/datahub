package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Primary-store direct-children checks for hierarchy delete gates. Search/index results are used
 * only for candidate discovery; parent relationships are verified from authoritative aspect reads.
 */
public final class AspectDirectChildrenWalker {

  private static final String PARENT_DOMAIN_INDEX_FIELD_NAME = "parentDomain.keyword";
  private static final String HAS_PARENT_DOMAIN_INDEX_FIELD_NAME = "hasParentDomain";
  private static final int PAGE_SIZE = 200;

  private AspectDirectChildrenWalker() {}

  /**
   * Returns whether {@code parentUrn} has any child domain entities in primary storage.
   *
   * <p>Uses search for candidate enumeration only. Each candidate is verified via {@code
   * domainProperties} from the primary store.
   */
  public static boolean hasDomainDirectChildren(
      @Nonnull OperationContext opContext,
      @Nonnull EntityClient entityClient,
      @Nonnull Urn parentUrn)
      throws RemoteInvocationException, URISyntaxException {
    SearchResult searchResult =
        entityClient.filter(
            opContext, DOMAIN_ENTITY_NAME, buildParentDomainFilter(parentUrn), null, 0, PAGE_SIZE);

    int returnedCount = searchResult.getEntities().size();
    // Only treat as truncated when the page is full; numEntities can exceed returnedCount when
    // ValidationUtils strips index ghosts (deleted entities) without adjusting numEntities.
    if (returnedCount >= PAGE_SIZE && searchResult.getNumEntities() > PAGE_SIZE) {
      return true;
    }

    Set<Urn> candidateUrns =
        searchResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toSet());
    if (candidateUrns.isEmpty()) {
      return false;
    }

    Map<Urn, EntityResponse> responses =
        entityClient.batchGetV2(
            opContext,
            DOMAIN_ENTITY_NAME,
            candidateUrns,
            Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME));

    return responses.values().stream().anyMatch(response -> parentMatches(response, parentUrn));
  }

  private static boolean parentMatches(@Nonnull EntityResponse response, @Nonnull Urn parentUrn) {
    if (!response.getAspects().containsKey(DOMAIN_PROPERTIES_ASPECT_NAME)) {
      return false;
    }
    DataMap data = response.getAspects().get(DOMAIN_PROPERTIES_ASPECT_NAME).getValue().data();
    DomainProperties properties = new DomainProperties(data);
    return properties.hasParentDomain() && parentUrn.equals(properties.getParentDomain());
  }

  @Nonnull
  private static Filter buildParentDomainFilter(@Nonnull Urn parentDomainUrn) {
    CriterionArray andArray =
        new CriterionArray(
            List.of(
                buildCriterion(HAS_PARENT_DOMAIN_INDEX_FIELD_NAME, Condition.EQUAL, "true"),
                buildCriterion(
                    PARENT_DOMAIN_INDEX_FIELD_NAME, Condition.EQUAL, parentDomainUrn.toString())));
    return new Filter()
        .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(andArray)));
  }
}
