package com.linkedin.datahub.graphql.types.schemafield;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import graphql.execution.DataFetcherResult;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SchemaFieldType implements com.linkedin.datahub.graphql.types.EntityType<SchemaFieldEntity, String> {

  public SchemaFieldType() { }

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
  public List<DataFetcherResult<SchemaFieldEntity>> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> schemaFieldUrns = urns.stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());

    try {
      return schemaFieldUrns.stream()
          .map(this::mapSchemaFieldUrn)
          .map(schemaFieldEntity -> DataFetcherResult.<SchemaFieldEntity>newResult()
              .data(schemaFieldEntity)
              .build()
          )
          .collect(Collectors.toList());

    } catch (Exception e) {
      throw new RuntimeException("Failed to load schemaField entity", e);
    }
  }

  private SchemaFieldEntity mapSchemaFieldUrn(Urn urn) {
    try {
      SchemaFieldEntity result = new SchemaFieldEntity();
      result.setUrn(urn.toString());
      result.setType(EntityType.SCHEMA_FIELD);
      result.setFieldPath(urn.getEntityKey().get(1));
      Urn parentUrn = Urn.createFromString(urn.getEntityKey().get(0));
      result.setParent(UrnToEntityMapper.map(parentUrn));
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load schemaField entity", e);
    }
  }

}

