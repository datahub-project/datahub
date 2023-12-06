package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class AuditStampMapper implements ModelMapper<com.linkedin.common.AuditStamp, AuditStamp> {

  public static final AuditStampMapper INSTANCE = new AuditStampMapper();

  public static AuditStamp map(@Nonnull final com.linkedin.common.AuditStamp auditStamp) {
    return INSTANCE.apply(auditStamp);
  }

  @Override
  public AuditStamp apply(@Nonnull final com.linkedin.common.AuditStamp auditStamp) {
    final AuditStamp result = new AuditStamp();
    result.setActor(auditStamp.getActor().toString());
    result.setTime(auditStamp.getTime());
    return result;
  }
}
