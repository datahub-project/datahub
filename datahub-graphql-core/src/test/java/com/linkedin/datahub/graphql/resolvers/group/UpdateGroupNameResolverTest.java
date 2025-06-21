package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateNameInput;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateNameResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateGroupNameResolverTest {

  private EntityService _entityService;
  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private QueryContext _mockContext;
  private UpdateNameResolver _resolver;
  private static final String TEST_GROUP_URN = "urn:li:corpGroup:testGroup";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testUser";

  @BeforeMethod
  public void setupTest() {
    _entityService = mock(EntityService.class);
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _mockContext = getMockAllowContext();
    _resolver = new UpdateNameResolver(_entityService, _entityClient);
    UpdateNameInput input = new UpdateNameInput();
    input.setName("new name");
    input.setUrn(TEST_GROUP_URN);

    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
  }

  @Test
  public void testUpdateGroupNameWithExistingInfo() throws Exception {
    // Setup
    CorpGroupUrn groupUrn = CorpGroupUrn.createFromString(TEST_GROUP_URN);
    CorpGroupInfo existingInfo = new CorpGroupInfo();
    existingInfo.setDisplayName("Old Name");

    when(_entityService.exists(any(), eq(groupUrn), eq(true))).thenReturn(true);
    when(_entityService.getAspect(any(), eq(groupUrn), eq("corpGroupInfo"), eq(0)))
        .thenReturn(existingInfo);

    // Execute
    CompletableFuture<Boolean> result = _resolver.get(_dataFetchingEnvironment);

    // Verify
    assertTrue(result.get());
    verify(_entityService)
        .ingestProposal(any(), any(MetadataChangeProposal.class), any(AuditStamp.class), eq(false));
  }

  @Test
  public void testUpdateGroupNameWithNullInfo() throws Exception {
    // Setup
    CorpGroupUrn groupUrn = CorpGroupUrn.createFromString(TEST_GROUP_URN);

    when(_entityService.exists(any(), eq(groupUrn), eq(true))).thenReturn(true);
    when(_entityService.getAspect(any(), eq(groupUrn), eq("corpGroupInfo"), eq(0)))
        .thenReturn(null);

    // Execute
    CompletableFuture<Boolean> result = _resolver.get(_dataFetchingEnvironment);

    // Verify
    assertTrue(result.get());
    verify(_entityService)
        .ingestProposal(any(), any(MetadataChangeProposal.class), any(AuditStamp.class), eq(false));
  }
}
