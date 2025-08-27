package com.linkedin.datahub.graphql.resolvers.link;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.url.Url;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AddLinkInput;
import com.linkedin.datahub.graphql.resolvers.mutate.AddLinkResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AddLinkResolverTest {
  private static final String ASSET_URN = "urn:li:dataset:(test1,test2,test3)";
  private static final String TEST_URL = "https://www.github.com";
  private static final String TEST_LABEL = "Test Label";
  private static final AddLinkInput TEST_INPUT = new AddLinkInput(TEST_URL, TEST_LABEL, ASSET_URN);

  private void setupTest(DataFetchingEnvironment mockEnv) {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testGetSuccessNoPreviousAspect() throws Exception {
    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata link = new InstitutionalMemoryMetadata();
    link.setUrl(new Url(TEST_URL));
    link.setDescription(TEST_LABEL);
    elements.add(link);
    expectedAspect.setElements(elements);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetSuccessWithPreviousAspect() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata link =
        LinkTestUtils.createLink("https://www.google.com", "Original Label");
    elements.add(link);
    originalAspect.setElements(elements);

    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray newElements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata newLink = LinkTestUtils.createLink(TEST_URL, TEST_LABEL);
    // make sure to include existing link
    newElements.add(link);
    newElements.add(newLink);
    expectedAspect.setElements(newElements);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetFailureNoEntity() throws Exception {
    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(false, null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
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
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 0, null);
  }

  @Test
  public void testShouldNotAddLinkWithTheSameUrlAndLabel() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata link = LinkTestUtils.createLink(TEST_URL, TEST_LABEL);
    elements.add(link);
    originalAspect.setElements(elements);

    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray newElements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata newLink = LinkTestUtils.createLink(TEST_URL, TEST_LABEL);
    // should include only already existing link
    newElements.add(link);
    expectedAspect.setElements(newElements);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetSuccessWhenLinkWithTheSameUrlAndDifferentLabelAdded() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata link = LinkTestUtils.createLink(TEST_URL, "Another label");

    elements.add(link);
    originalAspect.setElements(elements);

    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray newElements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata newLink = LinkTestUtils.createLink(TEST_URL, TEST_LABEL);
    // make sure to include existing link
    newElements.add(link);
    newElements.add(newLink);
    expectedAspect.setElements(newElements);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetSuccessWhenLinkWithDifferentUrlAndTheSameLabelAdded() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata link =
        LinkTestUtils.createLink("https://another-url.com", TEST_LABEL);
    elements.add(link);
    originalAspect.setElements(elements);

    EntityClient mockClient = LinkTestUtils.initMockClient();
    EntityService<?> mockService = LinkTestUtils.initMockEntityService(true, originalAspect);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray newElements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata newLink = LinkTestUtils.createLink(TEST_URL, TEST_LABEL);
    // make sure to include existing link
    newElements.add(link);
    newElements.add(newLink);
    expectedAspect.setElements(newElements);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    LinkTestUtils.verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }
}
