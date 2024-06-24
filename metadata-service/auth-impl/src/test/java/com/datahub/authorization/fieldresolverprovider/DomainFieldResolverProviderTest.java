package com.datahub.authorization.fieldresolverprovider;

import static org.mockito.Mockito.mock;

import com.linkedin.entity.client.SystemEntityClient;

public class DomainFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<DomainFieldResolverProvider> {
  @Override
  protected DomainFieldResolverProvider buildFieldResolverProvider() {
    return new DomainFieldResolverProvider(mock(SystemEntityClient.class));
  }
}
