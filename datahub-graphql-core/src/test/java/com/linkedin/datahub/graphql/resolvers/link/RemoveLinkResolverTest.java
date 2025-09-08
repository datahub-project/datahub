package com.linkedin.datahub.graphql.resolvers.link;

import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.common.*;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RemoveLinkInput;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveLinkResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RemoveLinkResolverTest {
  private static final String ASSET_URN = "urn:li:dataset:(test1,test2,test3)";

  private static DataFetchingEnvironment initMockEnv(RemoveLinkInput input) {
    DataFetchingEnvironment mockEnv = LinkTestUtils.initMockEnv();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    return mockEnv;
  }

  @Test
  public void testGetSuccessWhenRemovingExistingLinkByUrlAndLabel() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata originalLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label");
    InstitutionalMemoryMetadata originalLinkWithAnotherLabel =
        LinkTestUtils.createLink("https://original-url.com", "Another label");
    InstitutionalMemoryMetadataArray elements =
        new InstitutionalMemoryMetadataArray(originalLink, originalLinkWithAnotherLabel);
    originalAspect.setElements(elements);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray expectedElements =
        new InstitutionalMemoryMetadataArray(originalLinkWithAnotherLabel);
    expectedAspect.setElements(expectedElements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(new RemoveLinkInput("https://original-url.com", "Original label", ASSET_URN));
    RemoveLinkResolver resolver = new RemoveLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetSuccessWhenRemovingExistingLinkByUrl() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata originalLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label");
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray(originalLink);
    originalAspect.setElements(elements);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray newElements = new InstitutionalMemoryMetadataArray();
    expectedAspect.setElements(newElements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(new RemoveLinkInput("https://original-url.com", null, ASSET_URN));
    RemoveLinkResolver resolver = new RemoveLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetSuccessWhenRemovingNotExistingLink() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();
    originalAspect.setElements(elements);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray newElements = new InstitutionalMemoryMetadataArray();
    expectedAspect.setElements(newElements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(new RemoveLinkInput("https://original-url.com", "Original label", ASSET_URN));
    RemoveLinkResolver resolver = new RemoveLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetFailureNoEntity() throws Exception {
    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(false, null);

    DataFetchingEnvironment mockEnv =
        initMockEnv(new RemoveLinkInput("https://original-url.com", "Original label", ASSET_URN));
    RemoveLinkResolver resolver = new RemoveLinkResolver(mockService, mockClient);
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
        .thenReturn(new RemoveLinkInput("https://original-url.com", "Original label", ASSET_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RemoveLinkResolver resolver = new RemoveLinkResolver(mockService, mockClient);
    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 0, null);
  }
}
