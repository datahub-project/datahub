package com.linkedin.metadata.client;

import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class JavaEntityClientTest {

    private EntityService _mockEntityService;
    private DeleteEntityService _deleteEntityService;
    private EntitySearchService _entitySearchService;
    private CachingEntitySearchService _cachingEntitySearchService;
    private SearchService _searchService;
    private LineageSearchService _lineageSearchService;
    private TimeseriesAspectService _timeseriesAspectService;
    private EventProducer _eventProducer;
    private RestliEntityClient _restliEntityClient;

    @BeforeMethod
    public void setupTest() {
        _mockEntityService = mock(EntityService.class);
        _deleteEntityService = mock(DeleteEntityService.class);
        _entitySearchService = mock(EntitySearchService.class);
        _cachingEntitySearchService = mock(CachingEntitySearchService.class);
        _searchService = mock(SearchService.class);
        _lineageSearchService = mock(LineageSearchService.class);
        _timeseriesAspectService = mock(TimeseriesAspectService.class);
        _eventProducer = mock(EventProducer.class);
        _restliEntityClient = mock(RestliEntityClient.class);
    }

    private JavaEntityClient getJavaEntityClient() {
        return new JavaEntityClient(
                _mockEntityService,
                _deleteEntityService,
                _entitySearchService,
                _cachingEntitySearchService,
                _searchService,
                _lineageSearchService,
                _timeseriesAspectService,
                _eventProducer,
                _restliEntityClient);
    }

    @Test
    void testSuccessAndNoRetries() {
        JavaEntityClient client = getJavaEntityClient();

        Supplier<Object> mockSupplier = mock(Supplier.class);

        when(mockSupplier.get())
                .thenReturn(42);

        assertEquals(client.withRetry(mockSupplier), 42);
        verify(mockSupplier, times(1)).get();
    }

    @Test
    void testSuccessAfterMultipleRetries() {
        JavaEntityClient client = getJavaEntityClient();

        Supplier<Object> mockSupplier = mock(Supplier.class);

        when(mockSupplier.get())
                .thenThrow(new IllegalStateException("error"))
                .thenThrow(new IllegalStateException("error"))
                .thenThrow(new IllegalStateException("error"))
                .thenReturn(42);

        assertEquals(client.withRetry(mockSupplier), 42);
        verify(mockSupplier, times(4)).get();
    }

    @Test
    void testThrowAfterMultipleRetries() {
        JavaEntityClient client = getJavaEntityClient();

        Supplier<Object> mockSupplier = mock(Supplier.class);

        when(mockSupplier.get())
                .thenThrow(new IllegalStateException("error"))
                .thenThrow(new IllegalStateException("error"))
                .thenThrow(new IllegalStateException("error"))
                .thenThrow(new IllegalStateException("error"));

        assertThrows(IllegalStateException.class, () -> client.withRetry(mockSupplier));
        verify(mockSupplier, times(4)).get();
    }
}