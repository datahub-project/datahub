package com.linkedin.datahub.graphql.types.columnview;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.DataHubColumnView;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewColumn;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewColumnDisplay;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewColumnType;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewDefinition;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewExpand;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewLabelParams;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewLabelStyle;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewOverflow;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewSort;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewStructuredPropertyParams;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewTarget;
import com.linkedin.datahub.graphql.generated.DataHubViewType;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.SortOrder;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.view.DataHubViewMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.view.DataHubColumnViewInfo;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Maps the {@code dataHubColumnViewInfo} aspect onto the GraphQL {@link DataHubColumnView}. */
@Slf4j
public class DataHubColumnViewMapper implements ModelMapper<EntityResponse, DataHubColumnView> {

  public static final DataHubColumnViewMapper INSTANCE = new DataHubColumnViewMapper();

  public static DataHubColumnView map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataHubColumnView apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataHubColumnView result = new DataHubColumnView();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATAHUB_COLUMN_VIEW);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataHubColumnView> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME, this::mapInfo);
    return mappingHelper.getResult();
  }

  private void mapInfo(@Nonnull final DataHubColumnView view, @Nonnull final DataMap dataMap) {
    final DataHubColumnViewInfo info = new DataHubColumnViewInfo(dataMap);
    view.setName(info.getName());
    view.setDescription(info.getDescription());
    view.setViewType(DataHubViewType.valueOf(info.getType().toString()));
    view.setTarget(DataHubColumnViewTarget.valueOf(info.getTarget().toString()));
    view.setDefinition(mapDefinition(info.getDefinition()));
    view.setCreated(mapAuditStamp(info.getCreated()));
    view.setLastModified(mapAuditStamp(info.getLastModified()));
  }

  @Nonnull
  public static DataHubColumnViewDefinition mapDefinition(
      @Nonnull final com.linkedin.view.DataHubColumnViewDefinition definition) {
    final DataHubColumnViewDefinition result = new DataHubColumnViewDefinition();
    result.setColumns(
        definition.getColumns().stream()
            .map(DataHubColumnViewMapper::mapColumn)
            .collect(Collectors.toList()));
    if (definition.hasSort()) {
      final DataHubColumnViewSort sort = new DataHubColumnViewSort();
      sort.setColumn(mapColumn(definition.getSort().getColumn()));
      sort.setOrder(SortOrder.valueOf(definition.getSort().getOrder().toString()));
      result.setSort(sort);
    }
    if (definition.hasFilter()) {
      // Row filters reuse the Views filter shape (AND/OR of FacetFilters).
      result.setFilter(DataHubViewMapper.mapFilter(definition.getFilter()));
    }
    return result;
  }

  /**
   * Column kind ↔ GraphQL shape lives here and in {@code ColumnViewUtils.mapColumn} (input side).
   * Referenced entities (structured property, glossary node) are emitted as unresolved stubs for
   * batch resolution under the caller's context (see
   * GmsGraphQLEngine.configureColumnViewResolvers).
   */
  @Nonnull
  public static DataHubColumnViewColumn mapColumn(
      @Nonnull final com.linkedin.view.DataHubColumnViewColumn column) {
    final DataHubColumnViewColumn result = new DataHubColumnViewColumn();
    result.setType(DataHubColumnViewColumnType.valueOf(column.getType().toString()));
    if (column.hasStructuredPropertyParams()) {
      final StructuredPropertyEntity property = new StructuredPropertyEntity();
      property.setUrn(column.getStructuredPropertyParams().getUrn().toString());
      property.setType(EntityType.STRUCTURED_PROPERTY);
      final DataHubColumnViewStructuredPropertyParams params =
          new DataHubColumnViewStructuredPropertyParams();
      params.setStructuredProperty(property);
      result.setStructuredPropertyParams(params);
    }
    if (column.hasLabelParams()) {
      // Stub of the concrete entity (Tag or GlossaryTerm) so EntityTypeResolver can batch-load it.
      final Urn labelUrn = column.getLabelParams().getUrn();
      final Entity label;
      if (TAG_ENTITY_NAME.equals(labelUrn.getEntityType())) {
        final Tag tag = new Tag();
        tag.setUrn(labelUrn.toString());
        tag.setType(EntityType.TAG);
        label = tag;
      } else {
        final GlossaryTerm term = new GlossaryTerm();
        term.setUrn(labelUrn.toString());
        term.setType(EntityType.GLOSSARY_TERM);
        label = term;
      }
      final DataHubColumnViewLabelParams params = new DataHubColumnViewLabelParams();
      params.setLabel(label);
      result.setLabelParams(params);
    }
    if (column.hasDisplay()) {
      result.setDisplay(mapDisplay(column.getDisplay()));
    }
    return result;
  }

  /** Presentation hints; every field optional, {@code custom} passed through untouched. */
  @Nonnull
  public static DataHubColumnViewColumnDisplay mapDisplay(
      @Nonnull final com.linkedin.view.DataHubColumnViewColumnDisplay display) {
    final DataHubColumnViewColumnDisplay result = new DataHubColumnViewColumnDisplay();
    if (display.hasWidth()) {
      result.setWidth(display.getWidth());
    }
    if (display.hasMaxItems()) {
      result.setMaxItems(display.getMaxItems());
    }
    if (display.hasLabelStyle()) {
      result.setLabelStyle(DataHubColumnViewLabelStyle.valueOf(display.getLabelStyle().name()));
    }
    if (display.hasOverflow()) {
      result.setOverflow(DataHubColumnViewOverflow.valueOf(display.getOverflow().name()));
    }
    if (display.hasExpand()) {
      result.setExpand(DataHubColumnViewExpand.valueOf(display.getExpand().name()));
    }
    if (display.hasCustom()) {
      result.setCustom(
          display.getCustom().entrySet().stream()
              .map(
                  e -> {
                    final StringMapEntry entry = new StringMapEntry();
                    entry.setKey(e.getKey());
                    entry.setValue(e.getValue());
                    return entry;
                  })
              .collect(Collectors.toList()));
    }
    return result;
  }

  @Nonnull
  private static AuditStamp mapAuditStamp(@Nonnull final com.linkedin.common.AuditStamp stamp) {
    final AuditStamp result = new AuditStamp();
    result.setTime(stamp.getTime());
    result.setActor(stamp.getActor().toString());
    return result;
  }
}
