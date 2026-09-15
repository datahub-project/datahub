package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
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

  @Mock private GroupService groupServiceMock;

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
    return new GroupMembershipFieldResolverProvider(groupServiceMock);
  }

  @Test
  public void shouldReturnGroupsMembershipType() {
    assertEquals(
        EntityFieldType.GROUP_MEMBERSHIP,
        groupMembershipFieldResolverProvider.getFieldTypes().get(0));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResponseIsNull() {
    when(groupServiceMock.getGroupsForUser(eq(systemOperationContext), any(Urn.class)))
        .thenReturn(Collections.emptyList());

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(groupServiceMock, times(1)).getGroupsForUser(eq(systemOperationContext), any(Urn.class));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResourceDoesNotBelongToAnyGroup() {
    when(groupServiceMock.getGroupsForUser(eq(systemOperationContext), any(Urn.class)))
        .thenReturn(Collections.emptyList());

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(groupServiceMock, times(1)).getGroupsForUser(eq(systemOperationContext), any(Urn.class));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenThereIsAnException() {
    when(groupServiceMock.getGroupsForUser(eq(systemOperationContext), any(Urn.class)))
        .thenThrow(new RuntimeException("fetch failed"));

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
    verify(groupServiceMock, times(1)).getGroupsForUser(eq(systemOperationContext), any(Urn.class));
  }

  @Test
  public void shouldReturnFieldValueWithOnlyGroupsOfTheResource() {
    when(groupServiceMock.getGroupsForUser(eq(systemOperationContext), any(Urn.class)))
        .thenReturn(List.of(UrnUtils.getUrn(CORPGROUP_URN)));

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertEquals(Set.of(CORPGROUP_URN), result.getFieldValuesFuture().join().getValues());
    verify(groupServiceMock, times(1)).getGroupsForUser(eq(systemOperationContext), any(Urn.class));
  }

  @Test
  public void shouldReturnFieldValueWithOnlyNativeGroupsOfTheResource() {
    when(groupServiceMock.getGroupsForUser(eq(systemOperationContext), any(Urn.class)))
        .thenReturn(List.of(UrnUtils.getUrn(NATIVE_CORPGROUP_URN)));

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertEquals(Set.of(NATIVE_CORPGROUP_URN), result.getFieldValuesFuture().join().getValues());
    verify(groupServiceMock, times(1)).getGroupsForUser(eq(systemOperationContext), any(Urn.class));
  }

  @Test
  public void shouldReturnFieldValueWithGroupsAndNativeGroupsOfTheResource() {
    when(groupServiceMock.getGroupsForUser(eq(systemOperationContext), any(Urn.class)))
        .thenReturn(
            ImmutableList.of(
                UrnUtils.getUrn(CORPGROUP_URN), UrnUtils.getUrn(NATIVE_CORPGROUP_URN)));

    var result =
        groupMembershipFieldResolverProvider.getFieldResolver(
            systemOperationContext, RESOURCE_SPEC);

    assertEquals(
        Set.of(CORPGROUP_URN, NATIVE_CORPGROUP_URN),
        result.getFieldValuesFuture().join().getValues());
    verify(groupServiceMock, times(1)).getGroupsForUser(eq(systemOperationContext), any(Urn.class));
  }
}
