package com.linkedin.datahub.graphql.types.thrift.mapper;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ThriftAnnotation;
import com.linkedin.datahub.graphql.generated.ThriftAnnotationValue;
import com.linkedin.datahub.graphql.generated.ThriftEnum;
import com.linkedin.datahub.graphql.generated.ThriftEnumItem;
import com.linkedin.datahub.graphql.generated.ThriftEnumItemValue;
import com.linkedin.datahub.graphql.generated.ThriftNamespace;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.schema.ThriftEnumKey;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps GMS response objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
@Slf4j
public class ThriftEnumMapper
  implements ModelMapper<EntityResponse, ThriftEnum> {

  public static final ThriftEnumMapper INSTANCE = new ThriftEnumMapper();

  public static ThriftEnum map(@Nonnull final EntityResponse thriftEnum) {
    return INSTANCE.apply(thriftEnum);
  }

  @Override
  public ThriftEnum apply(@Nonnull final EntityResponse entityResponse) {
    ThriftEnum result = new ThriftEnum();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.THRIFT_ENUM);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ThriftEnum> mappingHelper = new MappingHelper<>(
      aspectMap,
      result
    );
    mappingHelper.mapToResult(
      THRIFT_ENUM_KEY_ASPECT_NAME,
      this::mapThriftEnumKey
    );
    mappingHelper.mapToResult(
      THRIFT_ENUM_PROPERTIES_ASPECT_NAME,
      this::mapThriftEnumProperties
    );

    return mappingHelper.getResult();
  }

  private void mapThriftEnumKey(
    @Nonnull ThriftEnum thriftEnum,
    @Nonnull DataMap dataMap
  ) {
    final ThriftEnumKey gmsKey = new ThriftEnumKey(dataMap);
    thriftEnum.setName(gmsKey.getName());
  }

  private void mapThriftEnumProperties(
    @Nonnull ThriftEnum thriftEnum,
    @Nonnull DataMap dataMap
  ) {
    final com.linkedin.schema.ThriftEnumProperties gmsProperties = new com.linkedin.schema.ThriftEnumProperties(
      dataMap
    );
    thriftEnum.setItems(
      gmsProperties
        .getItems()
        .stream()
        .map(this::mapThriftEnumItem)
        .collect(Collectors.toList())
    );
    if (gmsProperties.getAnnotations() != null) {
      thriftEnum.setAnnotations(
        gmsProperties
          .getAnnotations()
          .stream()
          .map(this::mapAnnotation)
          .collect(Collectors.toList())
      );
    }
    if (gmsProperties.getNamespace_() != null) {
      thriftEnum.setNamespaces(
        gmsProperties
          .getNamespace_()
          .entrySet()
          .stream()
          .map(e -> new ThriftNamespace(e.getKey(), e.getValue()))
          .collect(Collectors.toList())
      );
    }
  }

  private ThriftEnumItem mapThriftEnumItem(
    @Nonnull com.linkedin.schema.ThriftEnumItem gmsItem
  ) {
    ThriftEnumItem item = new ThriftEnumItem();
    item.setKey(gmsItem.getKey());
    if (gmsItem.getValue() != null) {
      item.setValue(
        gmsItem.getValue().isInt()
          ? new ThriftEnumItemValue(gmsItem.getValue().getInt(), null)
          : new ThriftEnumItemValue(null, gmsItem.getValue().getString())
      );
    }
    if (gmsItem.getAnnotations() != null) {
      item.setAnnotations(
        gmsItem
          .getAnnotations()
          .stream()
          .map(this::mapAnnotation)
          .collect(Collectors.toList())
      );
    }
    return item;
  }

  private ThriftAnnotation mapAnnotation(
    @Nonnull com.linkedin.schema.ThriftAnnotation gmsAnnotation
  ) {
    ThriftAnnotation annotation = new ThriftAnnotation();
    annotation.setKey(gmsAnnotation.getKey());
    annotation.setValue(
      gmsAnnotation.getValue().isInt()
        ? new ThriftAnnotationValue(gmsAnnotation.getValue().getInt(), null)
        : new ThriftAnnotationValue(null, gmsAnnotation.getValue().getString())
    );
    return annotation;
  }
}
