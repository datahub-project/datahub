package com.linkedin.datahub.graphql.resolvers.link;

import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.common.*;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateLinkInput;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateLinkResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateLinkResolverTest {
  private static final String ASSET_URN = "urn:li:dataset:(test1,test2,test3)";

  private static DataFetchingEnvironment initMockEnv(UpdateLinkInput input) {
    DataFetchingEnvironment mockEnv = LinkTestUtils.initMockEnv();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    return mockEnv;
  }

  @Test
  public void testGetSuccessWhenUpdatingExistingLink() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata originalLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label");
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray(originalLink);
    originalAspect.setElements(elements);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata updatedLink =
        LinkTestUtils.createLink("https://updated-url.com", "Updated label");
    InstitutionalMemoryMetadataArray newElements =
        new InstitutionalMemoryMetadataArray(updatedLink);
    expectedAspect.setElements(newElements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpdateLinkInput(
                "https://original-url.com",
                "Original label",
                "https://updated-url.com",
                "Updated label",
                ASSET_URN));
    UpdateLinkResolver resolver = new UpdateLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetFailedWhenUpdatingNonExistingLink() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    originalAspect.setElements(new InstitutionalMemoryMetadataArray());

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpdateLinkInput(
                "https://original-url.com",
                "Original label",
                "https://updated-url.com",
                "Updated label",
                ASSET_URN));
    UpdateLinkResolver resolver = new UpdateLinkResolver(mockService, mockClient);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailedWhenUpdatedLinkIsNotUnique() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata originalLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label");
    InstitutionalMemoryMetadata duplicatedLink =
        LinkTestUtils.createLink("https://duplicated-url.com", "Duplicated label");
    InstitutionalMemoryMetadataArray elements =
        new InstitutionalMemoryMetadataArray(originalLink, duplicatedLink);
    originalAspect.setElements(elements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpdateLinkInput(
                "https://original-url.com",
                "Original label",
                "https://duplicated-url.com",
                "Duplicated label",
                ASSET_URN));
    UpdateLinkResolver resolver = new UpdateLinkResolver(mockService, mockClient);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailureNoEntity() throws Exception {
    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(false, null);

    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpdateLinkInput(
                "https://original-url.com",
                "Original label",
                "https://duplicated-url.com",
                "Duplicated label",
                ASSET_URN));
    UpdateLinkResolver resolver = new UpdateLinkResolver(mockService, mockClient);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 0, null);
  }

  @Test
  public void testGetFailureNoPermission() throws Exception {
    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(
            new UpdateLinkInput(
                "https://original-url.com",
                "Original label",
                "https://duplicated-url.com",
                "Duplicated label",
                ASSET_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateLinkResolver resolver = new UpdateLinkResolver(mockService, mockClient);
    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 0, null);
  }
}
