package com.linkedin.metadata.search.siblings;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.Siblings;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps {@link ESUtils#SIBLING_PRIMARY_NAME} on the search documents of non-primary dataset
 * siblings: the name of the group's primary member. Search uses it to hide a non-primary sibling
 * for typed queries only when the query also matches the primary's name, so a member whose name
 * differs from its primary's (a Glue table grouped under another one) stays findable by its own
 * name.
 *
 * <p>A no-op unless {@link SearchConfiguration#isHideNonPrimarySiblings()} is on, so deployments
 * that don't hide siblings do no extra reads or writes and never carry the field.
 *
 * <p>Writes happen on three events: a non-primary's {@code siblings} aspect (look up the primary's
 * name), and a primary's {@code siblings}, {@code datasetProperties} or {@code datasetKey} aspect
 * (push its name to every non-primary member). The second covers a non-primary indexed before its
 * primary, and renames of the primary.
 */
@Slf4j
public class SiblingPrimaryNameIndexer {

  private static final String NAME_FIELD = "name";

  private final ElasticSearchService elasticSearchService;
  @Nullable private final SearchConfiguration searchConfiguration;

  public SiblingPrimaryNameIndexer(
      @Nonnull final ElasticSearchService elasticSearchService,
      @Nullable final SearchConfiguration searchConfiguration) {
    this.elasticSearchService = elasticSearchService;
    this.searchConfiguration = searchConfiguration;
  }

  /** Called after an aspect of {@code urn} has been written to the search index. */
  public void onAspectIndexed(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nullable final RecordTemplate aspect) {
    onAspectIndexed(opContext, urn, aspectName, aspect, null);
  }

  /**
   * {@code previousAspect} lets a re-ingested primary whose name did not change skip the push. A
   * {@code siblings} aspect is always processed, so the restore-indices backfill, which restates
   * unchanged aspects, still fills the field.
   */
  public void onAspectIndexed(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nullable final RecordTemplate aspect,
      @Nullable final RecordTemplate previousAspect) {
    if (searchConfiguration == null
        || !searchConfiguration.isHideNonPrimarySiblings()
        || !DATASET_ENTITY_NAME.equals(urn.getEntityType())) {
      return;
    }
    try {
      if (SIBLINGS_ASPECT_NAME.equals(aspectName)) {
        if (aspect instanceof Siblings) {
          onSiblingsIndexed(opContext, urn, (Siblings) aspect);
        }
      } else if (DATASET_PROPERTIES_ASPECT_NAME.equals(aspectName)
          || DATASET_KEY_ASPECT_NAME.equals(aspectName)) {
        if (previousAspect != null
            && aspect != null
            && Objects.equals(
                aspect.data().get(NAME_FIELD), previousAspect.data().get(NAME_FIELD))) {
          return;
        }
        Siblings siblings = readSiblings(opContext, urn);
        if (siblings != null && siblings.isPrimary()) {
          pushNameToNonPrimaries(opContext, urn, siblings);
        }
      }
    } catch (RuntimeException e) {
      // The field only refines hiding; a failure must not fail indexing of the aspect itself.
      log.warn("Could not update {} for sibling group of {}", ESUtils.SIBLING_PRIMARY_NAME, urn, e);
    }
  }

  private void onSiblingsIndexed(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Siblings siblings) {
    if (siblings.isPrimary()) {
      pushNameToNonPrimaries(opContext, urn, siblings);
      return;
    }
    if (!siblings.hasSiblings()) {
      return;
    }
    for (Urn sibling : siblings.getSiblings()) {
      Siblings siblingAspect = readSiblings(opContext, sibling);
      if (siblingAspect != null && siblingAspect.isPrimary()) {
        writeNames(opContext, urn, primaryNames(opContext, sibling));
        return;
      }
    }
  }

  private void pushNameToNonPrimaries(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn primary,
      @Nonnull final Siblings primarySiblings) {
    if (!primarySiblings.hasSiblings() || primarySiblings.getSiblings().isEmpty()) {
      return;
    }
    List<String> names = primaryNames(opContext, primary);
    for (Urn sibling : primarySiblings.getSiblings()) {
      Siblings siblingAspect = readSiblings(opContext, sibling);
      if (siblingAspect != null && !siblingAspect.isPrimary()) {
        writeNames(opContext, sibling, names);
      }
    }
  }

  /**
   * The names search matches the primary on by default: its properties name and its key name (which
   * carries the database and schema, e.g. {@code db.schema.orders}).
   */
  @VisibleForTesting
  @Nonnull
  List<String> primaryNames(@Nonnull final OperationContext opContext, @Nonnull final Urn primary) {
    Map<String, Aspect> aspects =
        retriever(opContext)
            .getLatestAspectObjects(
                opContext,
                Set.of(primary),
                Set.of(DATASET_PROPERTIES_ASPECT_NAME, DATASET_KEY_ASPECT_NAME))
            .getOrDefault(primary, Map.of());
    return Stream.of(
            stringField(aspects.get(DATASET_PROPERTIES_ASPECT_NAME), NAME_FIELD),
            stringField(aspects.get(DATASET_KEY_ASPECT_NAME), NAME_FIELD))
        .filter(Objects::nonNull)
        .distinct()
        .collect(Collectors.toList());
  }

  @Nullable
  private static String stringField(@Nullable final Aspect aspect, @Nonnull final String field) {
    if (aspect == null) {
      return null;
    }
    DataMap data = aspect.data();
    Object value = data.get(field);
    return value instanceof String && !((String) value).isBlank() ? (String) value : null;
  }

  @Nullable
  private Siblings readSiblings(@Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    Aspect aspect =
        retriever(opContext).getLatestAspectObject(opContext, urn, SIBLINGS_ASPECT_NAME);
    return aspect == null ? null : new Siblings(aspect.data());
  }

  private void writeNames(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final List<String> names) {
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    if (names.isEmpty()) {
      document.putNull(ESUtils.SIBLING_PRIMARY_NAME);
    } else {
      ArrayNode values = document.putArray(ESUtils.SIBLING_PRIMARY_NAME);
      names.forEach(values::add);
    }
    elasticSearchService.upsertDocument(
        opContext,
        DATASET_ENTITY_NAME,
        document.toString(),
        opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn));
  }

  @Nonnull
  private static AspectRetriever retriever(@Nonnull final OperationContext opContext) {
    return opContext.getAspectRetriever();
  }
}
