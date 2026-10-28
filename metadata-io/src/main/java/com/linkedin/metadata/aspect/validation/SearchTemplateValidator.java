package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_SYSTEM_IDS;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.utils.CriterionUtils.buildConjunctiveCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.entity.GenericScrollIterator;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateChecks;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateHoles;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRoutes;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/** Protects system templates and rejects bodies that will not render. */
@Setter
@Getter
@Accessors(chain = true)
public class SearchTemplateValidator extends AspectPayloadValidator {
  /**
   * Revalidate at most this many children. Another page, or a last page past this cap, is rejected
   * instead of scrolling further on the write path.
   */
  private static final int MAX_CHILD_REVALIDATIONS = 100;

  @Nonnull private AspectPluginConfig config;
  @Nonnull private final SearchTemplateLoader loader;
  private final int maxLeafClauses;
  private final int maxRoutes;
  private final int maxRoutePatternLength;
  @Nonnull private final List<EntitySpec> entitySpecs;
  @Nonnull private final SearchTemplateComponentRegistry registry;

  public SearchTemplateValidator(
      @Nonnull SearchTemplateLoader loader, @Nonnull SearchConfiguration searchConfiguration) {
    this(loader, searchConfiguration, List.of(), SearchTemplateComponentRegistry.empty());
  }

  public SearchTemplateValidator(
      @Nonnull SearchTemplateLoader loader,
      @Nonnull SearchConfiguration searchConfiguration,
      @Nonnull Collection<EntitySpec> entitySpecs) {
    this(loader, searchConfiguration, entitySpecs, SearchTemplateComponentRegistry.empty());
  }

  public SearchTemplateValidator(
      @Nonnull SearchTemplateLoader loader,
      @Nonnull SearchConfiguration searchConfiguration,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull SearchTemplateComponentRegistry registry) {
    this.loader = loader;
    this.entitySpecs = List.copyOf(entitySpecs);
    this.registry = registry;
    boolean configured =
        searchConfiguration.getV3() != null && searchConfiguration.getV3().getTemplates() != null;
    this.maxLeafClauses =
        configured ? searchConfiguration.getV3().getTemplates().getMaxLeafClauses() : 0;
    this.maxRoutes = configured ? searchConfiguration.getV3().getTemplates().getMaxRoutes() : 0;
    this.maxRoutePatternLength =
        configured ? searchConfiguration.getV3().getTemplates().getMaxRoutePatternLength() : 0;
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    Map<String, SearchTemplateProperties> batch = proposed(mcpItems);
    Set<String> deleted = deleted(mcpItems);
    for (BatchItem item : mcpItems) {
      try {
        validateItem(operationContext, item, retrieverContext, batch, deleted);
      } catch (SearchTemplateException e) {
        exceptions.addException(AspectValidationException.forItem(item, e.getMessage()));
      }
    }
    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    Map<String, SearchTemplateProperties> batch = proposed(changeMCPs);
    Set<String> deleted = deleted(changeMCPs);
    for (ChangeMCP item : changeMCPs) {
      if (!(item.getRecordTemplate() instanceof SearchTemplateProperties properties)) {
        continue;
      }
      try {
        validateBody(operationContext, item.getUrn(), properties, retrieverContext, batch, deleted);
      } catch (SearchTemplateException e) {
        exceptions.addException(AspectValidationException.forItem(item, e.getMessage()));
      }
    }
    return exceptions.streamAllExceptions();
  }

  private void validateItem(
      OperationFingerprint operationContext,
      BatchItem item,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted) {
    boolean systemActor = isSystemActor(item);
    boolean systemId = isSystemId(item.getUrn());
    if (ChangeType.DELETE.equals(item.getChangeType())) {
      if (systemId) {
        throw new SearchTemplateException("System search templates cannot be deleted");
      }
      SearchTemplateProperties stored = read(operationContext, item.getUrn(), retrieverContext);
      if (stored != null && !stored.isEditable() && !systemActor) {
        throw new SearchTemplateException("Search template is not editable");
      }
      // A child keeps this URN as parent. Deleting it fails the child's next load.
      for (Urn child : childrenOf(item.getUrn(), retrieverContext)) {
        String childUrn = child.toString();
        if (deleted.contains(childUrn) || reparented(batch, childUrn, item.getUrn())) {
          continue;
        }
        throw new SearchTemplateException("Search template is still a parent of " + child);
      }
      // A child created in this batch is not in the index yet.
      if (!batch.containsKey(item.getUrn().toString())) {
        for (Map.Entry<String, SearchTemplateProperties> proposed : batch.entrySet()) {
          String childUrn = proposed.getKey();
          if (deleted.contains(childUrn)) {
            continue;
          }
          SearchTemplateProperties child = proposed.getValue();
          if (child.hasParent() && item.getUrn().equals(child.getParent())) {
            throw new SearchTemplateException("Search template is still a parent of " + childUrn);
          }
        }
      }
      return;
    }
    if (systemId && !systemActor) {
      throw new SearchTemplateException("System search templates cannot be modified");
    }
    SearchTemplateProperties stored = read(operationContext, item.getUrn(), retrieverContext);
    if (stored != null && !stored.isEditable() && !systemActor) {
      throw new SearchTemplateException("Search template is not editable");
    }
    if (!(item.getRecordTemplate() instanceof SearchTemplateProperties properties)) {
      return;
    }
    validateParent(
        operationContext,
        item.getUrn(),
        properties,
        retrieverContext,
        batch,
        deleted,
        loader.maxParentDepth());
    SearchTemplateChecks.check(
        properties,
        maxLeafClauses,
        maxRoutes,
        maxRoutePatternLength,
        operationContext,
        retrieverContext.getAspectRetriever(),
        entitySpecs.isEmpty() ? null : entitySpecs,
        registry);
  }

  private void validateBody(
      OperationFingerprint operationContext,
      Urn urn,
      SearchTemplateProperties properties,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted) {
    validateParent(
        operationContext,
        urn,
        properties,
        retrieverContext,
        batch,
        deleted,
        loader.maxParentDepth());
    MergedSearchTemplate merged =
        loader.preview(
            operationContext,
            retrieverContext.getAspectRetriever(),
            urn.toString(),
            properties,
            batch);
    SearchTemplateChecks.checkMerged(
        merged,
        maxLeafClauses,
        maxRoutes,
        maxRoutePatternLength,
        operationContext,
        retrieverContext.getAspectRetriever(),
        entitySpecs.isEmpty() ? null : entitySpecs,
        registry);
    validateRoutes(operationContext, urn, merged, retrieverContext, batch, deleted);
    validateLadder(operationContext, urn, merged, retrieverContext, batch, deleted);
    validateFilledHoles(operationContext, urn, properties, retrieverContext, batch, deleted);
  }

  /**
   * A parent {@code query} write must keep every hole an inheriting child still fills. A child that
   * sets {@code query} stops that branch. The index can lag; rendering fails closed when a filled
   * hole is absent from the merged query.
   */
  private void validateFilledHoles(
      OperationFingerprint operationContext,
      Urn urn,
      SearchTemplateProperties properties,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted) {
    if (!properties.hasQuery()) {
      return;
    }
    walkChildren(operationContext, urn, batch, deleted, retrieverContext, new HashSet<>(), 0);
  }

  private void walkChildren(
      OperationFingerprint operationContext,
      Urn parent,
      Map<String, SearchTemplateProperties> overrides,
      @Nonnull Set<String> deleted,
      RetrieverContext retrieverContext,
      Set<String> visited,
      int depth) {
    if (depth > loader.maxParentDepth()) {
      throw new SearchTemplateException(
          "Search template parent chain exceeds " + loader.maxParentDepth());
    }
    for (Urn child : childrenOf(parent, retrieverContext)) {
      if (!visited.add(child.toString())) {
        continue;
      }
      SearchTemplateProperties childProperties =
          read(operationContext, child, retrieverContext, overrides, deleted);
      if (childProperties == null || childProperties.hasQuery()) {
        continue;
      }
      MergedSearchTemplate merged =
          loader.preview(
              operationContext,
              retrieverContext.getAspectRetriever(),
              child.toString(),
              childProperties,
              overrides);
      SearchTemplateHoles.requirePresent(merged.getQuery(), merged.getQueryFragments().keySet());
      walkChildren(
          operationContext, child, overrides, deleted, retrieverContext, visited, depth + 1);
    }
  }

  @Nonnull
  private static List<Urn> childrenOf(Urn parent, RetrieverContext retrieverContext) {
    Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(
            buildConjunctiveCriterion(
                buildCriterion("parent", Condition.EQUAL, parent.toString()))));
    GenericScrollIterator iterator =
        GenericScrollIterator.builder()
            .searchRetriever(retrieverContext.getSearchRetriever())
            .count(100)
            .filter(filter)
            .entities(List.of(SEARCH_TEMPLATE_ENTITY_NAME))
            .build();
    List<Urn> children = new ArrayList<>();
    while (iterator.hasNext()) {
      ScrollResult page = iterator.next();
      if (page.getEntities() == null || page.getEntities().isEmpty()) {
        break;
      }
      for (SearchEntity entity : page.getEntities()) {
        children.add(entity.getEntity());
      }
      boolean more = page.getScrollId() != null;
      if (children.size() > MAX_CHILD_REVALIDATIONS) {
        throw new SearchTemplateException(
            "Search template parent has more inheriting children than this write revalidates ("
                + MAX_CHILD_REVALIDATIONS
                + ")");
      }
      if (!more) {
        break;
      }
    }
    return children;
  }

  private static void validateRoutes(
      OperationFingerprint operationContext,
      Urn urn,
      MergedSearchTemplate merged,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted) {
    List<String> targets = new ArrayList<>();
    for (SearchTemplateRoutes.Compiled route : merged.getRoutes()) {
      targets.add(route.target());
    }
    if (merged.getDefaultRoute() != null) {
      targets.add(merged.getDefaultRoute());
    }
    if (targets.isEmpty()) {
      return;
    }
    SearchTemplateDialect dialect = SearchTemplateDialect.valueOf(merged.getDialect());
    String self = urn.toString();
    for (String target : targets) {
      if (self.equals(target)) {
        throw new SearchTemplateException("Search template route target is the template itself");
      }
      Urn parsed = UrnUtils.getUrn(target);
      if (!SEARCH_TEMPLATE_ENTITY_NAME.equals(parsed.getEntityType())) {
        throw new SearchTemplateException("Search template route target must be a searchTemplate");
      }
      SearchTemplateProperties found =
          read(operationContext, parsed, retrieverContext, batch, deleted);
      if (found == null) {
        throw new SearchTemplateException("Search template route target not found: " + target);
      }
      if (!dialect.equals(found.getDialect())) {
        throw new SearchTemplateException(
            "Search template route dialect is "
                + found.getDialect()
                + " and this template is "
                + dialect);
      }
    }
  }

  private void validateLadder(
      OperationFingerprint operationContext,
      Urn urn,
      MergedSearchTemplate merged,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted) {
    SearchTemplateLadder ladder = merged.getLadder();
    if (ladder == null) {
      return;
    }
    SearchTemplateDialect dialect = SearchTemplateDialect.valueOf(merged.getDialect());
    Set<String> path = new HashSet<>();
    path.add(urn.toString());
    walkLadder(
        operationContext,
        dialect,
        ladder,
        path,
        retrieverContext,
        batch,
        deleted,
        0,
        loader.maxParentDepth());
  }

  private void walkLadder(
      OperationFingerprint operationContext,
      SearchTemplateDialect dialect,
      SearchTemplateLadder ladder,
      Set<String> path,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted,
      int depth,
      int maxParentDepth) {
    if (depth > maxParentDepth) {
      throw new SearchTemplateException("Search template ladder exceeds " + maxParentDepth);
    }
    if (ladder.hasNext() && ladder.getNext() != null) {
      visitLadderLink(
          operationContext,
          dialect,
          ladder.getNext(),
          false,
          path,
          retrieverContext,
          batch,
          deleted,
          depth,
          maxParentDepth);
    }
    if (ladder.hasParallel() && ladder.getParallel() != null) {
      for (Urn member : ladder.getParallel()) {
        visitLadderLink(
            operationContext,
            dialect,
            member,
            true,
            path,
            retrieverContext,
            batch,
            deleted,
            depth,
            maxParentDepth);
      }
    }
  }

  /**
   * Follows the merged ladder. A child that does not set one keeps its parent's, which is what
   * search loads. A parallel member stays one query: select and stop are kept, and {@code next} or
   * another parallel group is rejected.
   */
  private void visitLadderLink(
      OperationFingerprint operationContext,
      SearchTemplateDialect dialect,
      Urn link,
      boolean parallelMember,
      Set<String> path,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted,
      int depth,
      int maxParentDepth) {
    if (!SEARCH_TEMPLATE_ENTITY_NAME.equals(link.getEntityType())) {
      throw new SearchTemplateException("Search template ladder target must be a searchTemplate");
    }
    if (path.contains(link.toString())) {
      throw new SearchTemplateException("Search template ladder cycle at " + link);
    }
    SearchTemplateProperties target =
        read(operationContext, link, retrieverContext, batch, deleted);
    if (target == null) {
      throw new SearchTemplateException("Search template ladder target not found: " + link);
    }
    if (!dialect.equals(target.getDialect())) {
      throw new SearchTemplateException(
          "Search template ladder dialect is "
              + target.getDialect()
              + " and this template is "
              + dialect);
    }
    MergedSearchTemplate merged =
        loader.preview(
            operationContext,
            retrieverContext.getAspectRetriever(),
            link.toString(),
            target,
            batch);
    SearchTemplateLadder effective = merged.getLadder();
    if (effective == null || SearchTemplateLoader.emptyLadder(effective)) {
      return;
    }
    if (parallelMember) {
      boolean next = effective.hasNext() && effective.getNext() != null;
      boolean nested =
          effective.hasParallel()
              && effective.getParallel() != null
              && !effective.getParallel().isEmpty();
      if (next || nested) {
        throw new SearchTemplateException(
            "Search template parallel member must be one query: " + link);
      }
      return;
    }
    path.add(link.toString());
    try {
      walkLadder(
          operationContext,
          dialect,
          effective,
          path,
          retrieverContext,
          batch,
          deleted,
          depth + 1,
          maxParentDepth);
    } finally {
      path.remove(link.toString());
    }
  }

  /**
   * Parent existence is read from the search index. A custom child can be saved while a parent
   * delete is not yet visible. Shipped templates are not editable, so that window is limited to
   * custom templates with more than one level.
   */
  private static void validateParent(
      OperationFingerprint operationContext,
      Urn urn,
      SearchTemplateProperties properties,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted,
      int maxParentDepth) {
    if (!properties.hasParent() || properties.getParent() == null) {
      return;
    }
    Set<String> seen = new HashSet<>();
    seen.add(urn.toString());
    Urn parent = properties.getParent();
    for (int depth = 1; depth <= maxParentDepth; depth++) {
      if (!SEARCH_TEMPLATE_ENTITY_NAME.equals(parent.getEntityType())) {
        throw new SearchTemplateException("Search template parent must be a searchTemplate");
      }
      if (!seen.add(parent.toString())) {
        throw new SearchTemplateException("Search template parent cycle at " + parent);
      }
      SearchTemplateProperties parentProperties =
          read(operationContext, parent, retrieverContext, batch, deleted);
      if (parentProperties == null) {
        throw new SearchTemplateException("Search template parent not found: " + parent);
      }
      if (!properties.getDialect().equals(parentProperties.getDialect())) {
        throw new SearchTemplateException(
            "Search template parent dialect is "
                + parentProperties.getDialect()
                + " and this template is "
                + properties.getDialect());
      }
      if (!parentProperties.hasParent() || parentProperties.getParent() == null) {
        return;
      }
      parent = parentProperties.getParent();
      if (depth == maxParentDepth) {
        throw new SearchTemplateException("Search template parent chain exceeds " + maxParentDepth);
      }
    }
  }

  @Nullable
  private static SearchTemplateProperties read(
      OperationFingerprint operationContext, Urn urn, RetrieverContext retrieverContext) {
    return read(operationContext, urn, retrieverContext, Map.of(), Set.of());
  }

  @Nullable
  private static SearchTemplateProperties read(
      OperationFingerprint operationContext,
      Urn urn,
      RetrieverContext retrieverContext,
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull Set<String> deleted) {
    String key = urn.toString();
    SearchTemplateProperties proposed = batch.get(key);
    // A body in this batch replaces a delete of the same URN.
    if (proposed != null) {
      return proposed;
    }
    if (deleted.contains(key)) {
      return null;
    }
    Aspect aspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(operationContext, urn, SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
    if (aspect == null || aspect.data() == null) {
      return null;
    }
    return new SearchTemplateProperties(aspect.data());
  }

  /**
   * A proposed aspect in this batch that no longer names {@code parent} has left. A child with no
   * proposed aspect still uses the indexed parent.
   */
  private static boolean reparented(
      @Nonnull Map<String, SearchTemplateProperties> batch,
      @Nonnull String childUrn,
      @Nonnull Urn parent) {
    SearchTemplateProperties proposed = batch.get(childUrn);
    if (proposed == null) {
      return false;
    }
    return !proposed.hasParent() || !parent.equals(proposed.getParent());
  }

  @Nonnull
  private static Set<String> deleted(@Nonnull Collection<? extends BatchItem> items) {
    Set<String> deleted = new HashSet<>();
    for (BatchItem item : items) {
      if (ChangeType.DELETE.equals(item.getChangeType())) {
        deleted.add(item.getUrn().toString());
      }
    }
    return deleted;
  }

  @Nonnull
  private static Map<String, SearchTemplateProperties> proposed(
      @Nonnull Collection<? extends BatchItem> items) {
    Map<String, SearchTemplateProperties> batch = new HashMap<>();
    for (BatchItem item : items) {
      if (item.getRecordTemplate() instanceof SearchTemplateProperties properties) {
        batch.put(item.getUrn().toString(), properties);
      }
    }
    return batch;
  }

  private static boolean isSystemId(Urn urn) {
    return SEARCH_TEMPLATE_ENTITY_NAME.equals(urn.getEntityType())
        && urn.getEntityKey().size() == 1
        && SEARCH_TEMPLATE_SYSTEM_IDS.contains(urn.getEntityKey().get(0));
  }

  private static boolean isSystemActor(BatchItem item) {
    return item.getAuditStamp() != null
        && item.getAuditStamp().getActor() != null
        && SYSTEM_ACTOR.equals(item.getAuditStamp().getActor().toString());
  }
}
