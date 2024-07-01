package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GroupMembershipFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<GroupMembershipFieldResolverProvider> {

  private static final String CORPGROUP_URN = "urn:li:corpGroup:groupname";
  private static final String NATIVE_CORPGROUP_URN = "urn:li:corpGroup:nativegroupname";
  private static final String RESOURCE_URN =
      "urn:li:dataset:(urn:li:dataPlatform:testPlatform,testDataset,PROD)";
  private static final EntitySpec RESOURCE_SPEC = new EntitySpec(DATASET_ENTITY_NAME, RESOURCE_URN);

  @Mock private SystemEntityClient entityClientMock;

  private OperationContext systemOperationContext;

  private GroupMembershipFieldResolverProvider groupMembershipFieldResolverProvider;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
    groupMembershipFieldResolverProvider = buildFieldResolverProvider();
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Override
  protected GroupMembershipFieldResolverProvider buildFieldResolverProvider() {
    return new GroupMembershipFieldResolverProvider(entityClientMock);
  }

  @Test
  public void shouldReturnGroupsMembershipType() {
    assertEquals(
        EntityFieldType.GROUP_MEMBERSHIP,
        groupMembershipFieldResolverProvider.getFieldTypes().get(0));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResponseIsNull()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(null);

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(entityClientMock, times(1))
        .getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResourceDoesNotBelongToAnyGroup()
      throws RemoteInvocationException, URISyntaxException {
    var entityResponseMock = mock(EntityResponse.class);
    when(entityResponseMock.getAspects()).thenReturn(new EnvelopedAspectMap());
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(entityClientMock, times(1))
        .getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenThereIsAnException()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(entityClientMock, times(1))
        .getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)));
  }

  @Test
  public void shouldReturnFieldValueWithOnlyGroupsOfTheResource()
      throws RemoteInvocationException, URISyntaxException {

    var groupMembership =
        new GroupMembership()
            .setGroups(new UrnArray(ImmutableList.of(Urn.createFromString(CORPGROUP_URN))));
    var entityResponseMock = mock(EntityResponse.class);
    var envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        GROUP_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(groupMembership.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertEquals(Set.of(CORPGROUP_URN), result.getFieldValuesFuture().join().getValues());
    verify(entityClientMock, times(1))
        .getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)));
  }

  @Test
  public void shouldReturnFieldValueWithOnlyNativeGroupsOfTheResource()
      throws RemoteInvocationException, URISyntaxException {

    var nativeGroupMembership =
        new NativeGroupMembership()
            .setNativeGroups(
                new UrnArray(ImmutableList.of(Urn.createFromString(NATIVE_CORPGROUP_URN))));
    var entityResponseMock = mock(EntityResponse.class);
    var envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(nativeGroupMembership.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertEquals(Set.of(NATIVE_CORPGROUP_URN), result.getFieldValuesFuture().join().getValues());
    verify(entityClientMock, times(1))
        .getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)));
  }

  @Test
  public void shouldReturnFieldValueWithGroupsAndNativeGroupsOfTheResource()
      throws RemoteInvocationException, URISyntaxException {

    var groupMembership =
        new GroupMembership()
            .setGroups(new UrnArray(ImmutableList.of(Urn.createFromString(CORPGROUP_URN))));
    var nativeGroupMembership =
        new NativeGroupMembership()
            .setNativeGroups(
                new UrnArray(ImmutableList.of(Urn.createFromString(NATIVE_CORPGROUP_URN))));
    var entityResponseMock = mock(EntityResponse.class);
    var envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        GROUP_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(groupMembership.data())));
    envelopedAspectMap.put(
        NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(nativeGroupMembership.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertEquals(
        Set.of(CORPGROUP_URN, NATIVE_CORPGROUP_URN),
        result.getFieldValuesFuture().join().getValues());
    verify(entityClientMock, times(1))
        .getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            any(Urn.class),
            eq(ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)));
  }
}
