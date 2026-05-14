package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteRoleResolverTest {

  private static final String CUSTOM_ROLE_URN = "urn:li:dataHubRole:custom-uuid";
  private static final String BUILT_IN_ROLE_URN = "urn:li:dataHubRole:Admin";

  private EntityClient _entityClient;
  private DeleteRoleResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _resolver = new DeleteRoleResolver(_entityClient);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> _resolver.get(_dataFetchingEnvironment));
  }

  @Test
  public void testBuiltInRoleRejected() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(BUILT_IN_ROLE_URN);

    stubBatchGet(Urn.createFromString(BUILT_IN_ROLE_URN), /* editable */ false);

    // The strongest guarantee for built-in role stability across upgrades is the server-side
    // refusal to delete; the UI hides the action but cannot be the source of truth.
    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
    verify(_entityClient, never()).deleteEntity(any(), any(Urn.class));
  }

  @Test
  public void testCustomRoleDeletedSuccessfully() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(CUSTOM_ROLE_URN);

    final Urn roleUrn = Urn.createFromString(CUSTOM_ROLE_URN);
    stubBatchGet(roleUrn, /* editable */ true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).get());
    verify(_entityClient, times(1)).deleteEntity(any(), eq(roleUrn));
  }

  @Test
  public void testMissingRoleFails() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(CUSTOM_ROLE_URN);

    when(_entityClient.batchGetV2(any(), eq(DATAHUB_ROLE_ENTITY_NAME), any(), any()))
        .thenReturn(Collections.emptyMap());

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
    verify(_entityClient, never()).deleteEntity(any(), any(Urn.class));
  }

  private void stubBatchGet(Urn roleUrn, boolean editable) throws Exception {
    final DataHubRoleInfo info = new DataHubRoleInfo();
    info.setName("Existing Name");
    info.setDescription("Existing Description");
    info.setEditable(editable);

    final EntityResponse response =
        new EntityResponse()
            .setUrn(roleUrn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        DATAHUB_ROLE_INFO_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(info.data())))));

    when(_entityClient.batchGetV2(any(), eq(DATAHUB_ROLE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of(roleUrn, response));
  }
}
