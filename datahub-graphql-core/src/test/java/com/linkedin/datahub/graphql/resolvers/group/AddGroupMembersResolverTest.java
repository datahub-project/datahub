package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddGroupMembersInput;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AddGroupMembersResolverTest {
  private static final String GROUP_URN_STRING = "urn:li:corpGroup:testNewGroup";
  private static final String USER_URN_STRING = "urn:li:corpuser:test";

  private static Urn _groupUrn;

  private GroupService _groupService;
  private AddGroupMembersResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _groupUrn = Urn.createFromString(GROUP_URN_STRING);

    _groupService = mock(GroupService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);
    AddGroupMembersInput input = new AddGroupMembersInput();
    input.setGroupUrn(GROUP_URN_STRING);
    input.setUserUrns(new ArrayList<>(Collections.singleton(USER_URN_STRING)));

    _resolver = new AddGroupMembersResolver(_groupService);

    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
  }

  @Test
  public void testFailsCannotManageUsersAndGroups() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsExternalGroup() {
    Origin groupOrigin = new Origin();
    groupOrigin.setType(OriginType.EXTERNAL);

    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);
    when(_groupService.groupExists(any(), any())).thenReturn(true);
    when(_groupService.getGroupOrigin(any(), eq(_groupUrn))).thenReturn(groupOrigin);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPassesNativeGroup() throws Exception {
    Origin groupOrigin = new Origin();
    groupOrigin.setType(OriginType.NATIVE);

    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);
    when(_groupService.groupExists(any(), any())).thenReturn(true);
    when(_groupService.getGroupOrigin(any(), eq(_groupUrn))).thenReturn(groupOrigin);

    _resolver.get(_dataFetchingEnvironment).join();
  }
}
