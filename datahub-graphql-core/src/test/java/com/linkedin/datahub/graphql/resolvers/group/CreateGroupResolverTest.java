/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateGroupInput;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateGroupResolverTest {
  private static final String GROUP_ID = "id";

  private GroupService _groupService;
  private CreateGroupResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;
  private CreateGroupInput _input;

  @BeforeMethod
  public void setupTest() {
    _groupService = mock(GroupService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);
    _input = new CreateGroupInput();
    _input.setId(GROUP_ID);

    _resolver = new CreateGroupResolver(_groupService);
  }

  @Test
  public void testFailsCannotManageUsersAndGroups() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPasses() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(_input);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_groupService.groupExists(any(), any())).thenReturn(false);

    _resolver.get(_dataFetchingEnvironment).join();
  }
}
