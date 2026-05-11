package com.linkedin.gms.factory.common;

import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.config.CurrentTenantProvider;

public class OperationContextTenantProvider implements CurrentTenantProvider {

  @Override
  public Object currentId() {
    return EbeanAspectDao.currentTenantId();
  }
}
