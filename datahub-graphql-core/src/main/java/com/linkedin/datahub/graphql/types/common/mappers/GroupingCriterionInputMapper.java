package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GroupingCriterion;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class GroupingCriterionInputMapper
    implements ModelMapper<GroupingCriterion, com.linkedin.metadata.query.GroupingCriterion> {

  public static final GroupingCriterionInputMapper INSTANCE = new GroupingCriterionInputMapper();

  public static com.linkedin.metadata.query.GroupingCriterion map(
      @Nullable QueryContext context, @Nonnull final GroupingCriterion groupingCriterion) {
    return INSTANCE.apply(context, groupingCriterion);
  }

  @Override
  public com.linkedin.metadata.query.GroupingCriterion apply(
      @Nullable QueryContext context, GroupingCriterion input) {
    return new com.linkedin.metadata.query.GroupingCriterion()
        .setBaseEntityType(
            input.getBaseEntityType() != null
                ? EntityTypeMapper.getName(input.getBaseEntityType())
                : null,
            SetMode.REMOVE_OPTIONAL_IF_NULL)
        .setGroupingEntityType(EntityTypeMapper.getName(input.getGroupingEntityType()));
  }
}
