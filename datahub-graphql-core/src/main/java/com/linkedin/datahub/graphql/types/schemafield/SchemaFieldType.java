package com.linkedin.datahub.graphql.types.schemafield;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SchemaFieldType
    implements com.linkedin.datahub.graphql.types.EntityType<SchemaFieldEntity, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          STRUCTURED_PROPERTIES_ASPECT_NAME,
          DEPRECATION_ASPECT_NAME,
          BUSINESS_ATTRIBUTE_ASPECT,
          DOCUMENTATION_ASPECT_NAME,
          STATUS_ASPECT_NAME);

  private final EntityClient _entityClient;
  private final FeatureFlags _featureFlags;

  @Override
  public EntityType type() {
    return EntityType.SCHEMA_FIELD;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<SchemaFieldEntity> objectClass() {
    return SchemaFieldEntity.class;
  }

  @Override
  public List<DataFetcherResult<SchemaFieldEntity>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> schemaFieldUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      Map<Urn, EntityResponse> entities = new HashMap<>();
      if (_featureFlags.isSchemaFieldEntityFetchEnabled()) {
        entities =
            _entityClient.batchGetV2(
                context.getOperationContext(),
                SCHEMA_FIELD_ENTITY_NAME,
                new HashSet<>(schemaFieldUrns),
                ASPECTS_TO_FETCH);
      }

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : schemaFieldUrns) {
        if (_featureFlags.isSchemaFieldEntityFetchEnabled()) {
          gmsResults.add(entities.getOrDefault(urn, null));
        } else {
          gmsResults.add(
              new EntityResponse()
                  .setUrn(urn)
                  .setAspects(new EnvelopedAspectMap())
                  .setEntityName(urn.getEntityType()));
        }
      }

      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<SchemaFieldEntity>newResult()
                          .data(SchemaFieldMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());

    } catch (Exception e) {
      throw new RuntimeException("Failed to load schemaField entity", e);
    }
  }
}
