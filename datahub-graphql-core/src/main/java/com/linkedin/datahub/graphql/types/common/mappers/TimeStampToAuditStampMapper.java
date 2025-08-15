package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.TimeStamp;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import javax.annotation.Nullable;

public class TimeStampToAuditStampMapper {

  public static final TimeStampToAuditStampMapper INSTANCE = new TimeStampToAuditStampMapper();

  public static AuditStamp map(
      @Nullable final QueryContext context, @Nullable final TimeStamp input) {
    if (input == null) {
      return null;
    }
    final AuditStamp result = new AuditStamp();
    result.setTime(input.getTime());
    if (input.hasActor()) {
      result.setActor(input.getActor().toString());
    }
    return result;
  }
}
