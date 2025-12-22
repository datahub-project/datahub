package com.linkedin.metadata.search;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.query.filter.Filter;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class SearchServiceSearchRetrieverTest {

  @Test
  public void testCountDelegatesCorrectlyToSearchService() {
    // Arrange
    SearchService mockSearchService = mock(SearchService.class);
    OperationContext mockOpContext = mock(OperationContext.class);
    Filter mockFilter = mock(Filter.class);

    List<String> entities = List.of("monitor", "dataset", "chart");

    Map<String, Long> countMap = new HashMap<>();
    countMap.put("monitor", 5L);
    countMap.put("dataset", 10L);
    countMap.put("chart", 3L);

    // Use eq() matchers to validate exact parameters are passed through
    when(mockSearchService.docCountPerEntity(eq(mockOpContext), eq(entities), eq(mockFilter)))
        .thenReturn(countMap);

    SearchServiceSearchRetriever retriever =
        SearchServiceSearchRetriever.builder()
            .systemOperationContext(mockOpContext)
            .searchService(mockSearchService)
            .build();

    // Act
    long result = retriever.count(entities, mockFilter);

    // Assert - verify counts are summed correctly
    assertEquals(result, 18L); // 5 + 10 + 3

    // Verify the search service was called with the exact parameters passed to count()
    verify(mockSearchService).docCountPerEntity(eq(mockOpContext), eq(entities), eq(mockFilter));
  }
}
