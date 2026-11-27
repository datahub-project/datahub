package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IntendedUse;
import com.linkedin.datahub.graphql.generated.IntendedUserType;
import com.linkedin.datahub.graphql.types.common.mappers.util.UnknownEnumValues;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.utils.UnknownDataGuard;
import javax.annotation.Nullable;
import lombok.NonNull;

public class IntendedUseMapper
    implements ModelMapper<com.linkedin.ml.metadata.IntendedUse, IntendedUse> {

  private static final UnknownDataGuard UNREPRESENTABLE =
      UnknownDataGuard.forSite(IntendedUseMapper.class, "intended use");

  public static final IntendedUseMapper INSTANCE = new IntendedUseMapper();

  public static IntendedUse map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.IntendedUse intendedUse) {
    return INSTANCE.apply(context, intendedUse);
  }

  @Override
  public IntendedUse apply(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.IntendedUse intendedUse) {
    final IntendedUse result = new IntendedUse();
    result.setOutOfScopeUses(intendedUse.getOutOfScopeUses());
    result.setPrimaryUses(intendedUse.getPrimaryUses());
    if (intendedUse.getPrimaryUsers() != null) {
      result.setPrimaryUsers(
          UnknownEnumValues.mapRepresentable(
              UNREPRESENTABLE,
              "intended user type",
              intendedUse.getPrimaryUsers(),
              v -> UnknownEnumValues.require(IntendedUserType.class, v)));
    }
    return result;
  }
}
