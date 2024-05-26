package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUserStatus;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CorpUserStatusMapper
    implements ModelMapper<com.linkedin.identity.CorpUserStatus, CorpUserStatus> {

  public static final CorpUserStatusMapper INSTANCE = new CorpUserStatusMapper();

  public static CorpUserStatus map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.identity.CorpUserStatus corpUserStatus) {
    return INSTANCE.apply(context, corpUserStatus);
  }

  @Override
  public CorpUserStatus apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.identity.CorpUserStatus status) {
    // Warning- if the backend provides an unexpected value this will fail.
    return CorpUserStatus.valueOf(status.getStatus());
  }
}
