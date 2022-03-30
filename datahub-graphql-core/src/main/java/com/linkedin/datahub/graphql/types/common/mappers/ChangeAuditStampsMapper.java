package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.ChangeAuditStamps;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;


public class ChangeAuditStampsMapper implements ModelMapper<com.linkedin.common.ChangeAuditStamps, ChangeAuditStamps> {
  public static final ChangeAuditStampsMapper INSTANCE = new ChangeAuditStampsMapper();

  public static ChangeAuditStamps map(com.linkedin.common.ChangeAuditStamps input) {
    return INSTANCE.apply(input);
  }

  @Override
  public ChangeAuditStamps apply(com.linkedin.common.ChangeAuditStamps input) {
    ChangeAuditStamps changeAuditStamps = new ChangeAuditStamps();
    changeAuditStamps.setCreated(AuditStampMapper.map(input.getCreated()));
    changeAuditStamps.setLastModified(AuditStampMapper.map(input.getLastModified()));
    if (input.hasDeleted()) {
      changeAuditStamps.setDeleted(AuditStampMapper.map(input.getDeleted()));
    }

    return changeAuditStamps;
  }
}
