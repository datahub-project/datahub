package com.datahub.authorization.fieldresolverprovider;

import static org.mockito.Mockito.mock;

import com.linkedin.entity.client.SystemEntityClient;

public class OwnerFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<OwnerFieldResolverProvider> {
  @Override
  protected OwnerFieldResolverProvider buildFieldResolverProvider() {
    return new OwnerFieldResolverProvider(mock(SystemEntityClient.class));
  }
}
