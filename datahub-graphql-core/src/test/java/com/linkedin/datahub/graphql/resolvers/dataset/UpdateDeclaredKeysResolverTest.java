package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContextWithOperationContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DeclaredBusinessKeyInput;
import com.linkedin.datahub.graphql.generated.DeclaredKeysInput;
import com.linkedin.dataset.DeclaredKeys;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDeclaredKeysResolverTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:s3,my_bucket/events,PROD)";

  private EntityClient entityClient;
  private UpdateDeclaredKeysResolver resolver;
  private DataFetchingEnvironment env;

  @BeforeMethod
  public void setUp() throws Exception {
    entityClient = mock(EntityClient.class);
    when(entityClient.exists(any(), any(Urn.class))).thenReturn(true);
    resolver = new UpdateDeclaredKeysResolver(entityClient);
    env = mock(DataFetchingEnvironment.class);
    DeclaredKeysInput input = new DeclaredKeysInput();
    input.setPrimaryKey(List.of("id"));
    DeclaredBusinessKeyInput businessKey = new DeclaredBusinessKeyInput();
    businessKey.setName("natural");
    businessKey.setFields(List.of("col_a", "as_of_date"));
    input.setBusinessKeys(List.of(businessKey));
    when(env.getArgument(eq("urn"))).thenReturn(DATASET_URN);
    when(env.getArgument(eq("input"))).thenReturn(input);
  }

  @Test
  public void testAuthorizedWritesDeclaredKeys() throws Exception {
    QueryContext context = getMockAllowContext();
    when(env.getContext()).thenReturn(context);

    assertTrue(resolver.get(env).join());

    ArgumentCaptor<MetadataChangeProposal> proposal =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityClient).ingestProposal(any(), proposal.capture(), anyBoolean());
    assertEquals(proposal.getValue().getAspectName(), Constants.DECLARED_KEYS_ASPECT_NAME);
    assertEquals(proposal.getValue().getEntityUrn(), UrnUtils.getUrn(DATASET_URN));
    DeclaredKeys written =
        RecordUtils.toRecordTemplate(
            DeclaredKeys.class,
            proposal
                .getValue()
                .getAspect()
                .getValue()
                .asString(java.nio.charset.StandardCharsets.UTF_8));
    assertEquals(written.getPrimaryKey(), List.of("id"));
    assertEquals(written.getBusinessKeys().get(0).getFields(), List.of("col_a", "as_of_date"));
    assertTrue(written.hasCreated());
    assertTrue(written.hasLastModified());
  }

  @Test
  public void testEmptyPrimaryKeyClearsIt() throws Exception {
    QueryContext context = getMockAllowContext();
    when(env.getContext()).thenReturn(context);
    DeclaredKeysInput input = new DeclaredKeysInput();
    input.setPrimaryKey(List.of());
    when(env.getArgument(eq("input"))).thenReturn(input);

    resolver.get(env).join();

    ArgumentCaptor<MetadataChangeProposal> proposal =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityClient).ingestProposal(any(), proposal.capture(), anyBoolean());
    DeclaredKeys written =
        RecordUtils.toRecordTemplate(
            DeclaredKeys.class,
            proposal
                .getValue()
                .getAspect()
                .getValue()
                .asString(java.nio.charset.StandardCharsets.UTF_8));
    assertFalse(written.hasPrimaryKey());
    assertFalse(written.hasBusinessKeys());
  }

  @Test
  public void testUnauthorizedIsDeniedWithoutWriting() throws Exception {
    QueryContext context = getMockDenyContextWithOperationContext();
    when(env.getContext()).thenReturn(context);

    CompletionException thrown = expectThrows(CompletionException.class, resolver.get(env)::join);

    assertTrue(thrown.getCause() instanceof AuthorizationException);
    verify(entityClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }
}
