package com.linkedin.metadata.shared;

import com.linkedin.common.UrnArray;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntityArray;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidationUtils {

  public static SearchResult validateSearchResult(
      final SearchResult searchResult, @Nonnull final EntityService entityService) {
    if (searchResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    SearchResult validatedSearchResult =
        new SearchResult()
            .setFrom(searchResult.getFrom())
            .setMetadata(searchResult.getMetadata())
            .setPageSize(searchResult.getPageSize())
            .setNumEntities(searchResult.getNumEntities());

    SearchEntityArray validatedEntities =
        searchResult.getEntities().stream()
            .filter(searchEntity -> entityService.exists(searchEntity.getEntity()))
            .collect(Collectors.toCollection(SearchEntityArray::new));
    validatedSearchResult.setEntities(validatedEntities);

    return validatedSearchResult;
  }

  public static ScrollResult validateScrollResult(
      final ScrollResult scrollResult, @Nonnull final EntityService entityService) {
    if (scrollResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    ScrollResult validatedScrollResult =
        new ScrollResult()
            .setMetadata(scrollResult.getMetadata())
            .setPageSize(scrollResult.getPageSize())
            .setNumEntities(scrollResult.getNumEntities());
    if (scrollResult.getScrollId() != null) {
      validatedScrollResult.setScrollId(scrollResult.getScrollId());
    }

    SearchEntityArray validatedEntities =
        scrollResult.getEntities().stream()
            .filter(searchEntity -> entityService.exists(searchEntity.getEntity()))
            .collect(Collectors.toCollection(SearchEntityArray::new));
    validatedScrollResult.setEntities(validatedEntities);

    return validatedScrollResult;
  }

  public static BrowseResult validateBrowseResult(
      final BrowseResult browseResult, @Nonnull final EntityService entityService) {
    if (browseResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    BrowseResult validatedBrowseResult =
        new BrowseResult()
            .setGroups(browseResult.getGroups())
            .setMetadata(browseResult.getMetadata())
            .setFrom(browseResult.getFrom())
            .setPageSize(browseResult.getPageSize())
            .setNumGroups(browseResult.getNumGroups())
            .setNumEntities(browseResult.getNumEntities())
            .setNumElements(browseResult.getNumElements());

    BrowseResultEntityArray validatedEntities =
        browseResult.getEntities().stream()
            .filter(browseResultEntity -> entityService.exists(browseResultEntity.getUrn()))
            .collect(Collectors.toCollection(BrowseResultEntityArray::new));
    validatedBrowseResult.setEntities(validatedEntities);

    return validatedBrowseResult;
  }

  public static ListResult validateListResult(
      final ListResult listResult, @Nonnull final EntityService entityService) {
    if (listResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    ListResult validatedListResult =
        new ListResult()
            .setStart(listResult.getStart())
            .setCount(listResult.getCount())
            .setTotal(listResult.getTotal());

    UrnArray validatedEntities =
        listResult.getEntities().stream()
            .filter(entityService::exists)
            .collect(Collectors.toCollection(UrnArray::new));
    validatedListResult.setEntities(validatedEntities);

    return validatedListResult;
  }

  public static LineageSearchResult validateLineageSearchResult(
      final LineageSearchResult lineageSearchResult, @Nonnull final EntityService entityService) {
    if (lineageSearchResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    LineageSearchResult validatedLineageSearchResult =
        new LineageSearchResult()
            .setMetadata(lineageSearchResult.getMetadata())
            .setFrom(lineageSearchResult.getFrom())
            .setPageSize(lineageSearchResult.getPageSize())
            .setNumEntities(lineageSearchResult.getNumEntities());

    LineageSearchEntityArray validatedEntities =
        lineageSearchResult.getEntities().stream()
            .filter(entity -> entityService.exists(entity.getEntity()))
            .collect(Collectors.toCollection(LineageSearchEntityArray::new));
    validatedLineageSearchResult.setEntities(validatedEntities);

    return validatedLineageSearchResult;
  }

  public static EntityLineageResult validateEntityLineageResult(
      @Nullable final EntityLineageResult entityLineageResult,
      @Nonnull final EntityService entityService) {
    if (entityLineageResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    final EntityLineageResult validatedEntityLineageResult =
        new EntityLineageResult()
            .setStart(entityLineageResult.getStart())
            .setCount(entityLineageResult.getCount())
            .setTotal(entityLineageResult.getTotal());

    final LineageRelationshipArray validatedRelationships =
        entityLineageResult.getRelationships().stream()
            .filter(relationship -> entityService.exists(relationship.getEntity()))
            .filter(relationship -> !entityService.isSoftDeleted(relationship.getEntity()))
            .collect(Collectors.toCollection(LineageRelationshipArray::new));

    validatedEntityLineageResult.setFiltered(
        (entityLineageResult.hasFiltered() && entityLineageResult.getFiltered() != null
                ? entityLineageResult.getFiltered()
                : 0)
            + entityLineageResult.getRelationships().size()
            - validatedRelationships.size());
    validatedEntityLineageResult.setRelationships(validatedRelationships);

    return validatedEntityLineageResult;
  }

  public static LineageScrollResult validateLineageScrollResult(
      final LineageScrollResult lineageScrollResult, @Nonnull final EntityService entityService) {
    if (lineageScrollResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    LineageScrollResult validatedLineageScrollResult =
        new LineageScrollResult()
            .setMetadata(lineageScrollResult.getMetadata())
            .setPageSize(lineageScrollResult.getPageSize())
            .setNumEntities(lineageScrollResult.getNumEntities());
    if (lineageScrollResult.getScrollId() != null) {
      validatedLineageScrollResult.setScrollId(lineageScrollResult.getScrollId());
    }

    LineageSearchEntityArray validatedEntities =
        lineageScrollResult.getEntities().stream()
            .filter(entity -> entityService.exists(entity.getEntity()))
            .collect(Collectors.toCollection(LineageSearchEntityArray::new));
    validatedLineageScrollResult.setEntities(validatedEntities);

    return validatedLineageScrollResult;
  }

  private ValidationUtils() {}
}
