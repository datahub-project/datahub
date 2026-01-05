package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.UserOrganizations;
import com.linkedin.metadata.utils.GenericRecordUtils;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RemoveUserFromOrganizationsResolverTest {

  private static final String USER_URN = "urn:li:corpuser:test";
  private static final String ORG_URN_1 = "urn:li:organization:org1";
  private static final String ORG_URN_2 = "urn:li:organization:org2";

  private EntityClient _entityClient;
  private RemoveUserFromOrganizationsResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _resolver = new RemoveUserFromOrganizationsResolver(_entityClient);
  }

  @Test
  public void testRemoveUserFromOrganizationsSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("userUrn"))).thenReturn(USER_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("organizationUrns")))
        .thenReturn(Arrays.asList(ORG_URN_1));
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    UserOrganizations existingUserOrgs = new UserOrganizations();
    UrnArray existingUrns = new UrnArray();
    existingUrns.add(Urn.createFromString(ORG_URN_1));
    existingUrns.add(Urn.createFromString(ORG_URN_2));
    existingUserOrgs.setOrganizations(existingUrns);

    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(
        new com.linkedin.entity.Aspect(
            GenericRecordUtils.serializeAspect(existingUserOrgs).data()));
    aspects.put(USER_ORGANIZATIONS_ASPECT_NAME, aspect);
    response.setAspects(aspects);

    Mockito.when(_entityClient.getV2(any(), any(), any(Urn.class), any())).thenReturn(response);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false))).thenReturn(null);

    Boolean result = _resolver.get(_dataFetchingEnvironment).get();

    assertTrue(result);
    ArgumentCaptor<com.linkedin.mxe.MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(com.linkedin.mxe.MetadataChangeProposal.class);
    Mockito.verify(_entityClient, Mockito.times(1))
        .ingestProposal(any(), proposalCaptor.capture(), eq(false));

    UserOrganizations updatedUserOrgs =
        GenericRecordUtils.deserializeAspect(
            proposalCaptor.getValue().getAspect().getValue(),
            proposalCaptor.getValue().getAspect().getContentType(),
            UserOrganizations.class);
    assertEquals(updatedUserOrgs.getOrganizations().size(), 1);
    assertEquals(updatedUserOrgs.getOrganizations().get(0).toString(), ORG_URN_2);
  }

  @Test
  public void testRemoveUserUnauthorized() {
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("userUrn"))).thenReturn(USER_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("organizationUrns")))
        .thenReturn(Arrays.asList(ORG_URN_1));
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testRemoveUserWithNoExistingOrganizations() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("userUrn"))).thenReturn(USER_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("organizationUrns")))
        .thenReturn(Arrays.asList(ORG_URN_1));
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    EntityResponse response = new EntityResponse();
    response.setAspects(new EnvelopedAspectMap());
    Mockito.when(_entityClient.getV2(any(), any(), any(Urn.class), any())).thenReturn(response);
    Mockito.when(_entityClient.ingestProposal(any(), any(), eq(false))).thenReturn(null);

    Boolean result = _resolver.get(_dataFetchingEnvironment).get();

    assertTrue(result);
    ArgumentCaptor<com.linkedin.mxe.MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(com.linkedin.mxe.MetadataChangeProposal.class);
    Mockito.verify(_entityClient, Mockito.times(1))
        .ingestProposal(any(), proposalCaptor.capture(), eq(false));

    UserOrganizations updatedUserOrgs =
        GenericRecordUtils.deserializeAspect(
            proposalCaptor.getValue().getAspect().getValue(),
            proposalCaptor.getValue().getAspect().getContentType(),
            UserOrganizations.class);
    assertEquals(updatedUserOrgs.getOrganizations().size(), 0);
  }
}
