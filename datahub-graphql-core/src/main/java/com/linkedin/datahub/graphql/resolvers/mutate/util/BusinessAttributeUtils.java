package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.businessattribute.BusinessAttributeAuthorizationUtils.isAuthorizedToEditBusinessAttribute;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.DateType;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.FixedType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import com.linkedin.schema.TimeType;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BusinessAttributeUtils {
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 1000;
  private static final String NAME_INDEX_FIELD_NAME = "name";

  private BusinessAttributeUtils() {}

  public static boolean hasNameConflict(
      String name, QueryContext context, EntityClient entityClient) {
    Filter filter = buildNameFilter(name);
    try {
      final SearchResult gmsResult =
          entityClient.filter(
              context.getOperationContext(),
              Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME,
              filter,
              null,
              DEFAULT_START,
              DEFAULT_COUNT);
      return gmsResult.getNumEntities() > 0;
    } catch (RemoteInvocationException e) {
      throw new RuntimeException("Failed to fetch Business Attributes", e);
    }
  }

  private static Filter buildNameFilter(String name) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(buildNameCriterion(name))));
  }

  private static CriterionArray buildNameCriterion(@Nonnull final String name) {
    return new CriterionArray(buildCriterion(NAME_INDEX_FIELD_NAME, Condition.EQUAL, name));
  }

  public static SchemaFieldDataType mapSchemaFieldDataType(
      com.linkedin.datahub.graphql.generated.SchemaFieldDataType type) {
    if (Objects.isNull(type)) {
      return null;
    }
    SchemaFieldDataType schemaFieldDataType = new SchemaFieldDataType();
    switch (type) {
      case BYTES:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new BytesType()));
        return schemaFieldDataType;
      case FIXED:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new FixedType()));
        return schemaFieldDataType;
      case ENUM:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new EnumType()));
        return schemaFieldDataType;
      case MAP:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new MapType()));
        return schemaFieldDataType;
      case TIME:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new TimeType()));
        return schemaFieldDataType;
      case BOOLEAN:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new BooleanType()));
        return schemaFieldDataType;
      case STRING:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new StringType()));
        return schemaFieldDataType;
      case NUMBER:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new NumberType()));
        return schemaFieldDataType;
      case DATE:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new DateType()));
        return schemaFieldDataType;
      case ARRAY:
        schemaFieldDataType.setType(SchemaFieldDataType.Type.create(new ArrayType()));
        return schemaFieldDataType;
      default:
        return null;
    }
  }

  public static void validateInputResources(List<ResourceRefInput> resources, QueryContext context)
      throws URISyntaxException {
    for (ResourceRefInput resource : resources) {
      if (!isAuthorizedToEditBusinessAttribute(context, resource.getResourceUrn())) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
    }
  }
}
