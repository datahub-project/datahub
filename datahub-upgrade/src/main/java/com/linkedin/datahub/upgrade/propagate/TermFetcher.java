package com.linkedin.datahub.upgrade.propagate;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class TermFetcher {
  private final OperationContext systemOpContext;
  private final EntityService<?> _entityService;
  private final EntitySearchService _entitySearchService;
  private final Set<String> allowedGlossaryNodes;

  private static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);

  public Set<Urn> fetchAllowedTerms(@Nonnull OperationContext opContext) {
    log.info("Fetching all terms");
    Set<Urn> allowedTerms = new HashSet<>();
    int batch = 0;
    String nextScrollId = null;
    do {
      batch++;
      ScrollResult scrollResult =
          _entitySearchService.scroll(
              systemOpContext,
              Collections.singletonList(Constants.GLOSSARY_TERM_ENTITY_NAME),
              null,
              null,
              1000,
              nextScrollId,
              PropagateTerms.ELASTIC_TIMEOUT,
              null);
      nextScrollId = scrollResult.getScrollId();
      log.info("Processing term batch {}", batch);
      Set<Urn> allowedTermsInBatch =
          filterAllowedTerms(
              opContext,
              scrollResult.getEntities().stream()
                  .map(SearchEntity::getEntity)
                  .collect(Collectors.toSet()));
      log.info("Found {} allowed terms", allowedTermsInBatch.size());
      allowedTerms.addAll(allowedTermsInBatch);
    } while (nextScrollId != null);

    log.info("Successfully fetched all terms. There are {} allowed terms", allowedTerms.size());
    return allowedTerms;
  }

  private boolean isAllowed(EntityResponse entityResponse) {
    if (!entityResponse.getAspects().containsKey(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)) {
      return false;
    }
    GlossaryTermInfo glossaryTermInfo =
        new GlossaryTermInfo(
            entityResponse
                .getAspects()
                .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
                .getValue()
                .data());
    if (glossaryTermInfo.getParentNode() == null) {
      return false;
    }
    return allowedGlossaryNodes.contains(glossaryTermInfo.getParentNode().toString());
  }

  private Set<Urn> filterAllowedTerms(@Nonnull OperationContext opContext, Set<Urn> terms) {
    try {
      return _entityService
          .getEntitiesV2(opContext, Constants.GLOSSARY_TERM_ENTITY_NAME, terms, ASPECTS_TO_FETCH)
          .values()
          .stream()
          .filter(this::isAllowed)
          .map(EntityResponse::getUrn)
          .collect(Collectors.toSet());
    } catch (URISyntaxException e) {
      log.error("Error while fetching term info for a batch of terms", e);
      return Collections.emptySet();
    }
  }
}
