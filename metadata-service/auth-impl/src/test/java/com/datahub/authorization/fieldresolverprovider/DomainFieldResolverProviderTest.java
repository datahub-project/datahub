package com.datahub.authorization.fieldresolverprovider;

import static org.mockito.Mockito.mock;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClient;

public class DomainFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<DomainFieldResolverProvider> {
  @Override
  protected DomainFieldResolverProvider buildFieldResolverProvider() {
    return new DomainFieldResolverProvider(mock(EntityClient.class), mock(Authentication.class));
  }
}
