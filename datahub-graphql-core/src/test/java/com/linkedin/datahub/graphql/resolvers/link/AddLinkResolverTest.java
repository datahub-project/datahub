package com.linkedin.datahub.graphql.resolvers.link;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.InstitutionalMemoryMetadataSettings;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AddLinkInput;
import com.linkedin.datahub.graphql.generated.LinkSettingsInput;
import com.linkedin.datahub.graphql.resolvers.mutate.AddLinkResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AddLinkResolverTest {
  private static final String ASSET_URN = "urn:li:dataset:(test1,test2,test3)";
  private static final String TEST_URL = "https://www.github.com";
  private static final String TEST_LABEL = "Test Label";
  private static final AddLinkInput TEST_INPUT =
      new AddLinkInput(TEST_URL, TEST_LABEL, ASSET_URN, new LinkSettingsInput(true));

  private void setupTest(DataFetchingEnvironment mockEnv) {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testGetSuccessNoPreviousAspect() throws Exception {
    EntityClient mockClient = initMockClient();
    EntityService<?> mockService = initMockEntityService(true, null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata link = new InstitutionalMemoryMetadata();
    InstitutionalMemoryMetadataSettings settings = new InstitutionalMemoryMetadataSettings();
    settings.setShowInAssetPreview(true);
    link.setUrl(new Url(TEST_URL));
    link.setDescription(TEST_LABEL);
    link.setSettings(settings);
    elements.add(link);
    expectedAspect.setElements(elements);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetSuccessWithPreviousAspect() throws Exception {
    InstitutionalMemory originalAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata link = new InstitutionalMemoryMetadata();
    InstitutionalMemoryMetadataSettings settings = new InstitutionalMemoryMetadataSettings();
    settings.setShowInAssetPreview(false);
    link.setUrl(new Url("https://www.google.com"));
    link.setDescription("Original Label");
    link.setSettings(settings);
    elements.add(link);
    originalAspect.setElements(elements);

    EntityClient mockClient = initMockClient();
    EntityService<?> mockService = initMockEntityService(true, originalAspect);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    InstitutionalMemory expectedAspect = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray newElements = new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadata newLink = new InstitutionalMemoryMetadata();
    InstitutionalMemoryMetadataSettings newSettings = new InstitutionalMemoryMetadataSettings();
    newSettings.setShowInAssetPreview(true);
    newLink.setUrl(new Url(TEST_URL));
    newLink.setDescription(TEST_LABEL);
    newLink.setSettings(newSettings);
    // make sure to include existing link
    newElements.add(link);
    newElements.add(newLink);
    expectedAspect.setElements(newElements);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    resolver.get(mockEnv).get();

    verifyIngestInstitutionalMemory(mockService, 1, expectedAspect);
  }

  @Test
  public void testGetFailureNoEntity() throws Exception {
    EntityClient mockClient = initMockClient();
    EntityService<?> mockService = initMockEntityService(false, null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    setupTest(mockEnv);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verifyIngestInstitutionalMemory(mockService, 0, null);
  }

  @Test
  public void testGetFailureNoPermission() throws Exception {
    EntityClient mockClient = initMockClient();
    EntityService<?> mockService = initMockEntityService(true, null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    AddLinkResolver resolver = new AddLinkResolver(mockService, mockClient);
    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());

    verifyIngestInstitutionalMemory(mockService, 0, null);
  }

  private EntityService<ChangeItemImpl> initMockEntityService(
      @Nonnull Boolean entityExists, @Nullable RecordTemplate currentAspect) {
    EntityService<ChangeItemImpl> mockService = Mockito.mock(EntityService.class);

    if (entityExists) {
      Mockito.when(
              mockService.exists(any(OperationContext.class), any(Urn.class), Mockito.eq(true)))
          .thenReturn(true);
    } else {
      Mockito.when(
              mockService.exists(any(OperationContext.class), any(Urn.class), Mockito.eq(true)))
          .thenReturn(false);
    }

    Mockito.when(
            mockService.getAspect(
                any(OperationContext.class),
                any(Urn.class),
                Mockito.eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                any(long.class)))
        .thenReturn(currentAspect);

    return mockService;
  }

  private EntityClient initMockClient() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(GLOSSARY_TERM_ENTITY_NAME),
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(GLOSSARY_TERM_ENTITY_NAME),
                Mockito.any(),
                Mockito.eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(new HashMap<>());

    return mockClient;
  }

  static void verifyIngestInstitutionalMemory(
      EntityService<?> mockService, int numberOfInvocations, InstitutionalMemory expectedAspect) {
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);

    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(any(), proposalCaptor.capture(), any(AuditStamp.class), Mockito.eq(false));

    if (numberOfInvocations > 0) {
      // check has time
      Assert.assertTrue(proposalCaptor.getValue().getSystemMetadata().getLastObserved() > 0L);

      InstitutionalMemory actualAspect =
          GenericRecordUtils.deserializeAspect(
              proposalCaptor.getValue().getAspect().getValue(),
              proposalCaptor.getValue().getAspect().getContentType(),
              InstitutionalMemory.class);

      Assert.assertEquals(actualAspect.getElements().size(), expectedAspect.getElements().size());

      actualAspect
          .getElements()
          .forEach(
              element -> {
                int index = actualAspect.getElements().indexOf(element);
                InstitutionalMemoryMetadata expectedElement =
                    expectedAspect.getElements().get(index);
                Assert.assertEquals(element.getUrl(), expectedElement.getUrl());
                Assert.assertEquals(element.getDescription(), expectedElement.getDescription());
                if (expectedElement.getSettings() != null) {
                  Assert.assertEquals(
                      element.getSettings().isShowInAssetPreview(),
                      expectedElement.getSettings().isShowInAssetPreview());
                }
              });
    }
  }
}
