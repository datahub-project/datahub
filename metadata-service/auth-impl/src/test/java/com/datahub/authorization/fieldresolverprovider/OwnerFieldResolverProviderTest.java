package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnerFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<OwnerFieldResolverProvider> {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
  private static final Urn USER_OWNER_URN = UrnUtils.getUrn("urn:li:corpuser:datahub");
  private static final Urn GROUP_OWNER_URN = UrnUtils.getUrn("urn:li:corpGroup:admins");
  private static final Urn OWNERSHIP_TYPE_URN =
      UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner");

  private SystemEntityClient mockEntityClient;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    opContext = mock(OperationContext.class);
  }

  @Override
  protected OwnerFieldResolverProvider buildFieldResolverProvider() {
    return new OwnerFieldResolverProvider(mock(SystemEntityClient.class));
  }

  @Test
  public void testResolvesOwnersAsBothStringAndTypedValues()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    final Owner userOwner = new Owner().setOwner(USER_OWNER_URN).setTypeUrn(OWNERSHIP_TYPE_URN);
    final Owner groupOwner = new Owner().setOwner(GROUP_OWNER_URN).setTypeUrn(OWNERSHIP_TYPE_URN);
    mockOwnershipResponse(new OwnerArray(userOwner, groupOwner));

    final FieldResolver.FieldValue result = resolveOwners();

    // String projection drives policy criterion matching.
    assertEquals(result.getValues(), Set.of(USER_OWNER_URN.toString(), GROUP_OWNER_URN.toString()));

    // Typed projection exposes the same owners as Owner records, so consumers (PolicyEngine's
    // ownership-type filtering) need not re-parse the strings.
    @SuppressWarnings("unchecked")
    final Set<Owner> typedValues = (Set<Owner>) result.getTypedValues();
    assertEquals(typedValues, Set.of(userOwner, groupOwner));
  }

  @Test
  public void testEmptyOwnershipAspectYieldsEmptyValues()
      throws ExecutionException,
          InterruptedException,
          RemoteInvocationException,
          URISyntaxException {
    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Set.of(OWNERSHIP_ASPECT_NAME)),
            eq(false)))
        .thenReturn(null);

    final FieldResolver.FieldValue result = resolveOwners();

    assertTrue(result.getValues().isEmpty());
    assertTrue(result.getTypedValues().isEmpty());
  }

  private FieldResolver.FieldValue resolveOwners() throws ExecutionException, InterruptedException {
    return new OwnerFieldResolverProvider(mockEntityClient)
        .getFieldResolver(opContext, new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString()))
        .getFieldValuesFuture()
        .get();
  }

  private void mockOwnershipResponse(OwnerArray owners)
      throws RemoteInvocationException, URISyntaxException {
    final Ownership ownership = new Ownership().setOwners(owners);
    final EnvelopedAspect ownershipAspect =
        new EnvelopedAspect().setValue(new Aspect(ownership.data()));
    final EntityResponse response =
        new EntityResponse()
            .setAspects(new EnvelopedAspectMap(Map.of(OWNERSHIP_ASPECT_NAME, ownershipAspect)));
    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Set.of(OWNERSHIP_ASPECT_NAME)),
            eq(false)))
        .thenReturn(response);
  }
}
