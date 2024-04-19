package com.datahub.authorization.fieldresolverprovider;

import static org.mockito.Mockito.mock;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClient;

public class OwnerFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<OwnerFieldResolverProvider> {
  @Override
  protected OwnerFieldResolverProvider buildFieldResolverProvider() {
    return new OwnerFieldResolverProvider(mock(EntityClient.class), mock(Authentication.class));
  }
}
