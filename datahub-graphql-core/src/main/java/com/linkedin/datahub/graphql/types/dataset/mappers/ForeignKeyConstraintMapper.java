package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.ForeignKeyConstraint;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Collectors;


@Slf4j
public class ForeignKeyConstraintMapper {
  private ForeignKeyConstraintMapper() { }

  public static ForeignKeyConstraint map(com.linkedin.schema.ForeignKeyConstraint constraint) {
    ForeignKeyConstraint result = new ForeignKeyConstraint();
    result.setName(constraint.getName());
    if (constraint.hasForeignDataset()) {
      result.setForeignDataset((Dataset) UrnToEntityMapper.map(constraint.getForeignDataset()));
    }
    if (constraint.hasSourceFields()) {
      result.setSourceFields(
          constraint.getSourceFields().stream().map(
              schemaFieldUrn -> mapSchemaFieldEntity(schemaFieldUrn)
          ).collect(Collectors.toList()));
    }
    if (constraint.hasForeignFields()) {
      result.setForeignFields(
          constraint.getForeignFields().stream().map(
              schemaFieldUrn -> mapSchemaFieldEntity(schemaFieldUrn)
          ).collect(Collectors.toList()));
    }
    return result;
  }

  private static SchemaFieldEntity mapSchemaFieldEntity(Urn schemaFieldUrn) {
    SchemaFieldEntity result = new SchemaFieldEntity();
    try {
      Urn resourceUrn = Urn.createFromString(schemaFieldUrn.getEntityKey().get(0));
      result.setParent(UrnToEntityMapper.map(resourceUrn));
    } catch (Exception e) {
      throw new RuntimeException("Error converting schemaField parent urn string to Urn", e);
    }
    result.setFieldPath(schemaFieldUrn.getEntityKey().get(1));
    return result;
  }
}
