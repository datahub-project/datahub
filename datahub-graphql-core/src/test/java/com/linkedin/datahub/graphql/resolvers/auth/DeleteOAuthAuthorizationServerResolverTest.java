package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteOAuthAuthorizationServerResolverTest {

  private static final String SERVER_URN = "urn:li:oauthAuthorizationServer:test-server";
  private static final Urn SERVER_URN_OBJ = UrnUtils.getUrn(SERVER_URN);

  @Mock private EntityClient entityClient;
  @Mock private DataFetchingEnvironment environment;

  private DeleteOAuthAuthorizationServerResolver resolver;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new DeleteOAuthAuthorizationServerResolver(entityClient);
  }

  @Test
  public void testConstructorNullCheck() {
    assertThrows(
        NullPointerException.class, () -> new DeleteOAuthAuthorizationServerResolver(null));
  }

  @Test
  public void testDeleteServerSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn(SERVER_URN);

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify
    assertTrue(result);
    verify(entityClient, times(1)).deleteEntity(any(), eq(SERVER_URN_OBJ));
  }

  @Test
  public void testDeleteServerUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn(SERVER_URN);

    // Expect AuthorizationException
    assertThrows(AuthorizationException.class, () -> resolver.get(environment));

    // Verify no deletion occurred
    verify(entityClient, never()).deleteEntity(any(), any());
  }

  @Test
  public void testDeleteNonOAuthServerUrnRejected() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn("urn:li:service:test-service");

    // Should throw IllegalArgumentException
    assertThrows(IllegalArgumentException.class, () -> resolver.get(environment));

    // Verify no deletion occurred
    verify(entityClient, never()).deleteEntity(any(), any());
  }

  @Test
  public void testDeleteDatasetUrnRejected() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn"))
        .thenReturn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    // Should throw IllegalArgumentException for wrong entity type
    assertThrows(IllegalArgumentException.class, () -> resolver.get(environment));

    // Verify no deletion occurred
    verify(entityClient, never()).deleteEntity(any(), any());
  }

  @Test
  public void testDeleteServerInvalidUrnFormat() {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn("not-a-valid-urn");

    // Should throw exception for invalid URN
    assertThrows(Exception.class, () -> resolver.get(environment));
  }

  @Test
  public void testDeleteServerNullUrn() {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn(null);

    // Should throw exception for null URN
    assertThrows(Exception.class, () -> resolver.get(environment));
  }

  @Test
  public void testDeleteServerEmptyUrn() {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn("");

    // Should throw exception for empty URN
    assertThrows(Exception.class, () -> resolver.get(environment));
  }
}
