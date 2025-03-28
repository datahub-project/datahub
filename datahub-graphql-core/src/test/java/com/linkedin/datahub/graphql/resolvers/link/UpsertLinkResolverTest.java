package com.linkedin.datahub.graphql.resolvers.link;

import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.common.*;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.LinkSettingsInput;
import com.linkedin.datahub.graphql.generated.UpsertLinkInput;
import com.linkedin.datahub.graphql.resolvers.mutate.UpsertLinkResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpsertLinkResolverTest {
  private static final String ASSET_URN = "urn:li:dataset:(test1,test2,test3)";

  private static DataFetchingEnvironment initMockEnv(UpsertLinkInput input) {
    DataFetchingEnvironment mockEnv = LinkTestUtils.initMockEnv();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    return mockEnv;
  }

  @Test
  public void testShouldCreateLinkWhenEntityHasNoLinks() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    originalAspect.setElements(new InstitutionalMemoryMetadataArray());

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata updatedLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label", false);
    InstitutionalMemoryMetadataArray newElements =
        new InstitutionalMemoryMetadataArray(updatedLink);
    expectedAspect.setElements(newElements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpsertLinkInput(
                "https://original-url.com",
                "Original label",
                ASSET_URN,
                new LinkSettingsInput(false)));
    UpsertLinkResolver resolver = new UpsertLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testShouldCreateLinkWhenUpsertLinkWithTheSameUrlAndDifferentLabel() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata originalLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label", true);
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray(originalLink);
    originalAspect.setElements(elements);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata updatedLink =
        LinkTestUtils.createLink("https://original-url.com", "New label", false);
    InstitutionalMemoryMetadataArray newElements =
        new InstitutionalMemoryMetadataArray(originalLink, updatedLink);
    expectedAspect.setElements(newElements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpsertLinkInput(
                "https://original-url.com", "New label", ASSET_URN, new LinkSettingsInput(false)));
    UpsertLinkResolver resolver = new UpsertLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testShouldCreateLinkWhenUpsertLinkWithDifferentUrlAndTheSameLabel() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata originalLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label", true);
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray(originalLink);
    originalAspect.setElements(elements);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata updatedLink =
        LinkTestUtils.createLink("https://new-url.com", "Original label", false);
    InstitutionalMemoryMetadataArray newElements =
        new InstitutionalMemoryMetadataArray(originalLink, updatedLink);
    expectedAspect.setElements(newElements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpsertLinkInput(
                "https://new-url.com", "Original label", ASSET_URN, new LinkSettingsInput(false)));
    UpsertLinkResolver resolver = new UpsertLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testShouldUpdateLinkWhenUpsertLinkWithTheSameUrlAndLabel() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata originalLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label", true);
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray(originalLink);
    originalAspect.setElements(elements);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadata updatedLink =
        LinkTestUtils.createLink("https://original-url.com", "Original label", false);
    InstitutionalMemoryMetadataArray newElements =
        new InstitutionalMemoryMetadataArray(updatedLink);
    expectedAspect.setElements(newElements);

    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    EntityClient mockClient = LinkTestUtils.initMockClient();
    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpsertLinkInput(
                "https://original-url.com",
                "Original label",
                ASSET_URN,
                new LinkSettingsInput(false)));
    UpsertLinkResolver resolver = new UpsertLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetFailureNoEntity() throws Exception {
    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(false, null);

    DataFetchingEnvironment mockEnv =
        initMockEnv(
            new UpsertLinkInput(
                "https://original-url.com",
                "Original label",
                ASSET_URN,
                new LinkSettingsInput(false)));
    UpsertLinkResolver resolver = new UpsertLinkResolver(mockService, mockClient);
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
            new UpsertLinkInput(
                "https://original-url.com",
                "Original label",
                ASSET_URN,
                new LinkSettingsInput(false)));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    UpsertLinkResolver resolver = new UpsertLinkResolver(mockService, mockClient);
    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 0, null);
  }
}
