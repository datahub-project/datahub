package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.index.query.TermQueryBuilder;
import org.testng.annotations.Test;

public class EntityTypeDocumentCountTest {

  @Test
  public void testCountsTheGivenEntityTypeInTheGivenIndex() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    CountResponse response = mock(CountResponse.class);
    when(response.getCount()).thenReturn(7L);
    when(client.count(any(), any(), any())).thenReturn(response);

    assertEquals(
        EntityTypeDocumentCount.count(client, "primaryindex_v3", "chart"), Long.valueOf(7));

    ArgumentCaptor<CountRequest> request = ArgumentCaptor.forClass(CountRequest.class);
    verify(client).count(any(), request.capture(), any());
    assertEquals(request.getValue().indices(), new String[] {"primaryindex_v3"});
    TermQueryBuilder term = (TermQueryBuilder) request.getValue().query();
    assertEquals(term.fieldName(), MappingConstants.ENTITY_TYPE_FIELD);
    assertEquals(term.value(), "chart");
  }

  @Test
  public void testFailedCountIsNull() throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.count(any(), any(), any())).thenThrow(new IOException("down"));

    assertNull(EntityTypeDocumentCount.count(client, "primaryindex_v3", "chart"));
  }
}
