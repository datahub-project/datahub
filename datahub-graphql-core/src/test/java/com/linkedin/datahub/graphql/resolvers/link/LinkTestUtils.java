package com.linkedin.datahub.graphql.resolvers.link;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;

public class LinkTestUtils {

  public static InstitutionalMemoryMetadata createLink(String url, String label) throws Exception {
    InstitutionalMemoryMetadata link = new InstitutionalMemoryMetadata();

    link.setUrl(new Url(url));
    link.setDescription(label);

    AuditStamp createStamp = new AuditStamp();
    createStamp.setActor(new Urn("urn:corpuser:test"));
    createStamp.setTime(Clock.systemUTC().millis());
    link.setCreateStamp(createStamp);

    return link;
  }

  public static DataFetchingEnvironment initMockEnv() {
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    return mockEnv;
  }

  public static EntityService<ChangeItemImpl> initMockEntityService(
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

  public static EntityClient initMockClient() throws Exception {
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
              });
    }
  }
}
