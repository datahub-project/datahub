package com.linkedin.gms.factory.common;

import io.ebean.config.TenantCatalogProvider;

public class OperationContextCatalogProvider implements TenantCatalogProvider {
  @Override
  public String catalog(Object tenantId) {
    return tenantId.toString();
  }
}
