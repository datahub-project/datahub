package com.linkedin.datahub.graphql.resolvers.columnview;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ColumnViewRelationshipsInput;
import com.linkedin.datahub.graphql.generated.FieldRelationshipPreview;
import com.linkedin.datahub.graphql.generated.RelatedFieldPreview;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.config.ColumnViewsConfiguration;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.service.ColumnViewService;
import com.linkedin.metadata.utils.columnview.ColumnViewColumnKinds;
import com.linkedin.view.DataHubColumnViewColumn;
import com.linkedin.view.DataHubColumnViewColumnType;
import com.linkedin.view.DataHubColumnViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Server-side preview of a relationship column for a page of schema fields. Pure projection under
 * the caller's context; previews are built by PARSING urns only (no entity hydration).
 *
 * <p>Caps: at most {@value #MAX_FIELD_URNS} fields per call; per-field item count clamped to the
 * configured {@code columnViews.relationshipPreviewLimit}; per-field total capped at {@value
 * #MAX_TOTAL} with {@code totalIsCapped} set.
 */
@Slf4j
public class ColumnViewRelationshipsResolver
    implements DataFetcher<CompletableFuture<List<FieldRelationshipPreview>>> {

  public static final int MAX_FIELD_URNS = 100;
  public static final int MAX_TOTAL = 1000;

  private final GraphService graphService;
  private final ColumnViewService columnViewService;
  private final ColumnViewsConfiguration config;

  public ColumnViewRelationshipsResolver(
      @Nonnull final GraphService graphService,
      @Nonnull final ColumnViewService columnViewService,
      @Nullable final ColumnViewsConfiguration config) {
    this.graphService = Objects.requireNonNull(graphService, "graphService must not be null");
    this.columnViewService =
        Objects.requireNonNull(columnViewService, "columnViewService must not be null");
    this.config = config;
  }

  @Override
  public CompletableFuture<List<FieldRelationshipPreview>> get(
      final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final ColumnViewRelationshipsInput input =
        bindArgument(environment.getArgument("input"), ColumnViewRelationshipsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final DataHubColumnViewColumnType type =
              DataHubColumnViewColumnType.valueOf(input.getColumn().name());
          final ColumnViewColumnKinds.Spec spec = ColumnViewColumnKinds.spec(type);
          if (!spec.isGraph()) {
            throw new IllegalArgumentException(
                String.format("Column kind %s is not a relationship column", type));
          }

          final int limit = previewLimit();
          int count = clamp(input.getCount() != null ? input.getCount() : limit, 1, limit);

          if (input.getColumnViewUrn() != null) {
            // Load the view under the caller's context; the column must be part of it.
            final Urn viewUrn = UrnUtils.getUrn(input.getColumnViewUrn());
            final DataHubColumnViewInfo info =
                columnViewService.getColumnViewInfo(context.getOperationContext(), viewUrn);
            if (info == null) {
              throw new IllegalArgumentException(
                  String.format("Column View with urn %s does not exist", viewUrn));
            }
            if (!ColumnViewUtils.canReadColumnView(info, context)) {
              throw new AuthorizationException(
                  "Unauthorized to use this Column View. Please contact your DataHub administrator.");
            }
            final DataHubColumnViewColumn column =
                info.getDefinition().getColumns().stream()
                    .filter(c -> c.getType() == type)
                    .findFirst()
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                String.format(
                                    "Column %s is not part of Column View %s", type, viewUrn)));
            if (column.hasDisplay() && column.getDisplay().hasMaxItems()) {
              count = clamp(column.getDisplay().getMaxItems(), 1, limit);
            }
          }

          final OperationContext opContext = context.getOperationContext();
          final List<Urn> fieldUrns =
              input.getFieldUrns().stream()
                  .distinct()
                  .limit(MAX_FIELD_URNS)
                  .map(UrnUtils::getUrn)
                  .collect(Collectors.toList());
          // This endpoint previews schema-field relationships only; it must not become a generic
          // relationship probe for arbitrary entities.
          fieldUrns.stream()
              .filter(urn -> !Constants.SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType()))
              .findFirst()
              .ifPresent(
                  urn -> {
                    throw new IllegalArgumentException(
                        String.format("fieldUrns must be schemaField urns; got %s", urn));
                  });
          if (fieldUrns.isEmpty()) {
            return Collections.emptyList();
          }

          // View Authorization: never disclose edges (or their count) for a field whose dataset
          // the caller cannot see. Such fields get an empty preview, not an error.
          final List<Urn> viewable =
              fieldUrns.stream()
                  .filter(urn -> canViewField(opContext, urn))
                  .collect(Collectors.toList());
          final Map<Urn, RelatedEntitiesResult> pages =
              viewable.isEmpty()
                  ? Collections.emptyMap()
                  : graphService.getRelatedEntitiesTopKPerSource(
                      opContext,
                      viewable,
                      Objects.requireNonNull(spec.getEdge()),
                      Objects.requireNonNull(spec.getDirection()),
                      count);

          return fieldUrns.stream()
              .map(urn -> toPreview(opContext, urn, pages.get(urn)))
              .collect(Collectors.toList());
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private int previewLimit() {
    final int configured = config != null ? config.getRelationshipPreviewLimit() : 5;
    return configured > 0 ? configured : 5;
  }

  private static int clamp(final int value, final int min, final int max) {
    return Math.max(min, Math.min(max, value));
  }

  @Nonnull
  private static FieldRelationshipPreview toPreview(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn fieldUrn,
      @Nullable final RelatedEntitiesResult page) {
    final FieldRelationshipPreview preview = new FieldRelationshipPreview();
    preview.setFieldUrn(fieldUrn.toString());
    if (page == null) {
      preview.setTotal(0L);
      preview.setTotalIsCapped(false);
      preview.setRelated(Collections.emptyList());
      return preview;
    }
    // Related entities the caller may not view are dropped BEFORE anything (name, platform) is
    // parsed out of their urn. The reported total is then a best-effort upper bound.
    final List<RelatedFieldPreview> related = new ArrayList<>();
    int hidden = 0;
    for (RelatedEntity entity : page.getEntities()) {
      final Urn relatedUrn = parseUrn(entity.getUrn());
      if (relatedUrn == null) {
        continue;
      }
      if (!canViewField(opContext, relatedUrn)) {
        hidden++;
        continue;
      }
      final RelatedFieldPreview mapped = toRelated(relatedUrn);
      if (mapped != null) {
        related.add(mapped);
      }
    }
    final long total = Math.max(related.size(), page.getTotal() - hidden);
    preview.setTotalIsCapped(total > MAX_TOTAL);
    preview.setTotal(Math.min(total, MAX_TOTAL));
    preview.setRelated(related);
    return preview;
  }

  /**
   * View Authorization gate for a field or related entity: the urn itself and, for schemaField
   * urns, the parent dataset must both be viewable. Short-circuits to true when View Authorization
   * is disabled (see {@link
   * com.linkedin.datahub.graphql.authorization.AuthorizationUtils#canView}).
   */
  static boolean canViewField(@Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    if (!canView(opContext, urn)) {
      return false;
    }
    if (Constants.SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())
        && urn.getEntityKey().size() >= 2) {
      final Urn parent = parseUrn(urn.getEntityKey().get(0));
      return parent == null || canView(opContext, parent);
    }
    return true;
  }

  @Nullable
  private static Urn parseUrn(@Nonnull final String urnString) {
    try {
      return Urn.createFromString(urnString);
    } catch (URISyntaxException e) {
      log.warn("Skipping unparseable related urn {}", urnString);
      return null;
    }
  }

  /**
   * Describe a related entity from its urn alone: schemaField urn {@code
   * urn:li:schemaField:(<datasetUrn>,<fieldPath>)} yields fieldPath + datasetUrn; dataset urns
   * yield platformUrn + name via {@link DatasetUrn}.
   */
  @Nullable
  static RelatedFieldPreview toRelated(@Nonnull final Urn urn) {
    final RelatedFieldPreview related = new RelatedFieldPreview();
    related.setUrn(urn.toString());
    related.setEntityType(EntityTypeMapper.getType(urn.getEntityType()));
    if (Constants.SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())
        && urn.getEntityKey().size() >= 2) {
      related.setFieldPath(urn.getEntityKey().get(1));
      try {
        final Urn parent = Urn.createFromString(urn.getEntityKey().get(0));
        related.setDatasetUrn(parent.toString());
        fillDataset(related, parent);
      } catch (URISyntaxException e) {
        log.debug("schemaField {} has an unparseable parent urn", urn);
      }
    } else if (Constants.DATASET_ENTITY_NAME.equals(urn.getEntityType())) {
      related.setDatasetUrn(urn.toString());
      fillDataset(related, urn);
    }
    return related;
  }

  private static void fillDataset(@Nonnull final RelatedFieldPreview related, @Nonnull Urn urn) {
    if (!Constants.DATASET_ENTITY_NAME.equals(urn.getEntityType())) {
      return;
    }
    try {
      final DatasetUrn datasetUrn = DatasetUrn.createFromUrn(urn);
      related.setDatasetName(datasetUrn.getDatasetNameEntity());
      related.setPlatformUrn(datasetUrn.getPlatformEntity().toString());
    } catch (URISyntaxException e) {
      log.debug("Dataset urn {} could not be parsed for name/platform", urn);
    }
  }
}
