package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.GroupingCriterion;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class GroupingCriterionInputMapper
    implements ModelMapper<GroupingCriterion, com.linkedin.metadata.query.GroupingCriterion> {

  public static final GroupingCriterionInputMapper INSTANCE = new GroupingCriterionInputMapper();

  public static com.linkedin.metadata.query.GroupingCriterion map(
      @Nonnull final GroupingCriterion groupingCriterion) {
    return INSTANCE.apply(groupingCriterion);
  }

  @Override
  public com.linkedin.metadata.query.GroupingCriterion apply(GroupingCriterion input) {
    return new com.linkedin.metadata.query.GroupingCriterion()
        .setRawEntityType(input.getRawEntityType())
        .setGroupingEntityType(input.getGroupingEntityType());
  }
}
