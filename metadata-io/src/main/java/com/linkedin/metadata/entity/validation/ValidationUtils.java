package com.linkedin.metadata.entity.validation;

import com.codahale.metrics.Timer;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.AbstractArrayTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.browse.BrowseResultEntityArray;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidationUtils {
  public static final int URN_NUM_BYTES_LIMIT = 512;
  public static final String URN_DELIMITER_SEPARATOR = "␟";

  /**
   * Validates a {@link RecordTemplate} and throws {@link
   * com.linkedin.restli.server.RestLiServiceException} if validation fails.
   *
   * @param record record to be validated.
   */
  public static void validateOrThrow(RecordTemplate record) {
    RecordTemplateValidator.validate(
        record,
        validationResult -> {
          throw new ValidationException(
              String.format(
                  "Failed to validate record with class %s: %s",
                  record.getClass().getName(), validationResult.getMessages().toString()));
        });
  }

  /**
   * Validates a {@link RecordTemplate} and logs a warning if validation fails.
   *
   * @param record record to be validated.ailure.
   */
  public static void validateOrWarn(RecordTemplate record) {
    RecordTemplateValidator.validate(
        record,
        validationResult -> {
          log.warn(String.format("Failed to validate record %s against its schema.", record));
        });
  }

  public static AspectSpec validate(EntitySpec entitySpec, String aspectName) {
    if (aspectName == null || aspectName.isEmpty()) {
      throw new UnsupportedOperationException(
          "Aspect name is required for create and update operations");
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format("Unknown aspect %s for entity %s", aspectName, entitySpec.getName()));
    }

    return aspectSpec;
  }

  public static void validateRecordTemplate(
      EntitySpec entitySpec,
      Urn urn,
      @Nullable RecordTemplate aspect,
      @Nonnull AspectRetriever aspectRetriever) {
    EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
    EntityRegistryUrnValidator validator = new EntityRegistryUrnValidator(entityRegistry);
    validator.setCurrentEntitySpec(entitySpec);
    Consumer<ValidationResult> resultFunction =
        validationResult -> {
          throw new IllegalArgumentException(
              "Invalid format for aspect: "
                  + entitySpec.getName()
                  + "\n Cause: "
                  + validationResult.getMessages());
        };

    RecordTemplateValidator.validate(
        EntityUtils.buildKeyAspect(entityRegistry, urn), resultFunction, validator);

    if (aspect != null) {
      RecordTemplateValidator.validate(aspect, resultFunction, validator);
    }
  }

  public static void validateUrn(@Nonnull EntityRegistry entityRegistry, @Nonnull final Urn urn) {
    EntityRegistryUrnValidator validator = new EntityRegistryUrnValidator(entityRegistry);
    validator.setCurrentEntitySpec(entityRegistry.getEntitySpec(urn.getEntityType()));
    RecordTemplateValidator.validate(
        EntityUtils.buildKeyAspect(entityRegistry, urn),
        validationResult -> {
          throw new IllegalArgumentException(
              "Invalid urn: " + urn + "\n Cause: " + validationResult.getMessages());
        },
        validator);

    if (urn.toString().trim().length() != urn.toString().length()) {
      throw new IllegalArgumentException(
          "Error: cannot provide an URN with leading or trailing whitespace");
    }
    if (URLEncoder.encode(urn.toString()).length() > URN_NUM_BYTES_LIMIT) {
      throw new IllegalArgumentException(
          "Error: cannot provide an URN longer than "
              + Integer.toString(URN_NUM_BYTES_LIMIT)
              + " bytes (when URL encoded)");
    }
    if (urn.toString().contains(URN_DELIMITER_SEPARATOR)) {
      throw new IllegalArgumentException(
          "Error: URN cannot contain " + URN_DELIMITER_SEPARATOR + " character");
    }
    try {
      Urn.createFromString(urn.toString());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static SearchResult validateSearchResult(
      final SearchResult searchResult, @Nonnull final EntityService<?> entityService) {
    try (Timer.Context ignored =
        MetricUtils.timer(ValidationUtils.class, "validateSearchResult").time()) {
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
          validateSearchUrns(
                  searchResult.getEntities(), SearchEntity::getEntity, entityService, true, true)
              .collect(Collectors.toCollection(SearchEntityArray::new));
      validatedSearchResult.setEntities(validatedEntities);

      return validatedSearchResult;
    }
  }

  public static ScrollResult validateScrollResult(
      final ScrollResult scrollResult, @Nonnull final EntityService<?> entityService) {
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
        validateSearchUrns(
                scrollResult.getEntities(), SearchEntity::getEntity, entityService, true, true)
            .collect(Collectors.toCollection(SearchEntityArray::new));

    validatedScrollResult.setEntities(validatedEntities);

    return validatedScrollResult;
  }

  public static BrowseResult validateBrowseResult(
      final BrowseResult browseResult, @Nonnull final EntityService<?> entityService) {
    try (Timer.Context ignored =
        MetricUtils.timer(ValidationUtils.class, "validateBrowseResult").time()) {
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
          validateSearchUrns(
                  browseResult.getEntities(), BrowseResultEntity::getUrn, entityService, true, true)
              .collect(Collectors.toCollection(BrowseResultEntityArray::new));
      validatedBrowseResult.setEntities(validatedEntities);

      return validatedBrowseResult;
    }
  }

  public static ListResult validateListResult(
      final ListResult listResult, @Nonnull final EntityService<?> entityService) {
    try (Timer.Context ignored =
        MetricUtils.timer(ValidationUtils.class, "validateListResult").time()) {
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
          validateSearchUrns(
                  listResult.getEntities(), Function.identity(), entityService, true, true)
              .collect(Collectors.toCollection(UrnArray::new));
      validatedListResult.setEntities(validatedEntities);

      return validatedListResult;
    }
  }

  public static LineageSearchResult validateLineageSearchResult(
      final LineageSearchResult lineageSearchResult,
      @Nonnull final EntityService<?> entityService) {
    try (Timer.Context ignored =
        MetricUtils.timer(ValidationUtils.class, "validateLineageResult").time()) {
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
          validateSearchUrns(
                  lineageSearchResult.getEntities(),
                  LineageSearchEntity::getEntity,
                  entityService,
                  true,
                  true)
              .collect(Collectors.toCollection(LineageSearchEntityArray::new));
      validatedLineageSearchResult.setEntities(validatedEntities);

      log.debug("Returning validated lineage search results");
      return validatedLineageSearchResult;
    }
  }

  public static EntityLineageResult validateEntityLineageResult(
      @Nullable final EntityLineageResult entityLineageResult,
      @Nonnull final EntityService<?> entityService) {
    if (entityLineageResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    final EntityLineageResult validatedEntityLineageResult =
        new EntityLineageResult()
            .setStart(entityLineageResult.getStart())
            .setCount(entityLineageResult.getCount())
            .setTotal(entityLineageResult.getTotal());

    LineageRelationshipArray validatedRelationships =
        validateSearchUrns(
                entityLineageResult.getRelationships(),
                LineageRelationship::getEntity,
                entityService,
                true,
                false)
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
      final LineageScrollResult lineageScrollResult,
      @Nonnull final EntityService<?> entityService) {
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
        validateSearchUrns(
                lineageScrollResult.getEntities(),
                LineageSearchEntity::getEntity,
                entityService,
                true,
                true)
            .collect(Collectors.toCollection(LineageSearchEntityArray::new));

    validatedLineageScrollResult.setEntities(validatedEntities);

    return validatedLineageScrollResult;
  }

  private static <T> Stream<T> validateSearchUrns(
      final AbstractArrayTemplate<T> array,
      Function<T, Urn> urnFunction,
      @Nonnull final EntityService<?> entityService,
      boolean enforceSQLExistence,
      boolean includeSoftDeleted) {

    if (enforceSQLExistence) {
      Set<Urn> existingUrns =
          entityService.exists(
              array.stream().map(urnFunction).collect(Collectors.toList()), includeSoftDeleted);
      return array.stream().filter(item -> existingUrns.contains(urnFunction.apply(item)));
    } else {
      Set<Urn> validatedUrns =
          array.stream()
              .map(urnFunction)
              .filter(
                  urn -> {
                    try {
                      validateUrn(entityService.getEntityRegistry(), urn);
                      return true;
                    } catch (Exception e) {
                      log.warn(
                          "Excluded {} from search result due to {}",
                          urn.toString(),
                          e.getMessage());
                    }
                    return false;
                  })
              .collect(Collectors.toSet());
      return array.stream().filter(item -> validatedUrns.contains(urnFunction.apply(item)));
    }
  }

  private ValidationUtils() {}
}
