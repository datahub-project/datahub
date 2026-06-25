package com.datahub.authorization.fieldresolverprovider;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.EntitySpec;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Set;
import org.testng.annotations.Test;

public class OwnerFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<OwnerFieldResolverProvider> {
  @Override
  protected OwnerFieldResolverProvider buildFieldResolverProvider() {
    return new OwnerFieldResolverProvider(mock(SystemEntityClient.class));
  }

  @Test
  public void testSharesPerRequestOwnershipCache() throws Exception {
    final SystemEntityClient entityClient = mock(SystemEntityClient.class);
    final OwnerFieldResolverProvider provider = new OwnerFieldResolverProvider(entityClient);
    final OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();

    final Urn resourceUrn = UrnUtils.getUrn("urn:li:dataset:ownerCacheTest");
    final Urn ownerUrn = UrnUtils.getUrn("urn:li:corpuser:datahub");

    final Ownership ownership =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    new Owner().setOwner(ownerUrn).setType(OwnershipType.TECHNICAL_OWNER)));
    final EntityResponse response = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(ownership.data())));
    response.setAspects(aspectMap);
    when(entityClient.getV2(
            eq(opContext),
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME))))
        .thenReturn(response);

    final EntitySpec spec = new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString());

    // Resolve the same resource twice within one request (sharing one OperationContext).
    final Set<String> first =
        provider.getFieldResolver(opContext, spec).getFieldValuesFuture().get().getValues();
    final Set<String> second =
        provider.getFieldResolver(opContext, spec).getFieldValuesFuture().get().getValues();

    assertEquals(first, Collections.singleton(ownerUrn.toString()));
    assertEquals(second, Collections.singleton(ownerUrn.toString()));

    // Shared per-request cache: ownership is fetched exactly once across both resolutions.
    verify(entityClient, times(1))
        .getV2(
            eq(opContext),
            eq(resourceUrn.getEntityType()),
            eq(resourceUrn),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME)));
  }
}
