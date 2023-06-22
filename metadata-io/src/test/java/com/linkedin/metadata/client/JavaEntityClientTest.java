package com.linkedin.metadata.client;

import com.codahale.metrics.Counter;
import com.linkedin.data.template.RequiredFieldNotPresentException;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.Supplier;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class JavaEntityClientTest {

    private EntityService _entityService;
    private DeleteEntityService _deleteEntityService;
    private EntitySearchService _entitySearchService;
    private CachingEntitySearchService _cachingEntitySearchService;
    private SearchService _searchService;
    private LineageSearchService _lineageSearchService;
    private TimeseriesAspectService _timeseriesAspectService;
    private EventProducer _eventProducer;
    private RestliEntityClient _restliEntityClient;
    private MockedStatic<MetricUtils> _metricUtils;
    private Counter _counter;

    @BeforeMethod
    public void setupTest() {
        _entityService = mock(EntityService.class);
        _deleteEntityService = mock(DeleteEntityService.class);
        _entitySearchService = mock(EntitySearchService.class);
        _cachingEntitySearchService = mock(CachingEntitySearchService.class);
        _searchService = mock(SearchService.class);
        _lineageSearchService = mock(LineageSearchService.class);
        _timeseriesAspectService = mock(TimeseriesAspectService.class);
        _eventProducer = mock(EventProducer.class);
        _restliEntityClient = mock(RestliEntityClient.class);
        _metricUtils = mockStatic(MetricUtils.class);
        _counter = mock(Counter.class);
        when(MetricUtils.counter(any(), any())).thenReturn(_counter);
    }

    @AfterMethod
    public void closeTest() {
        _metricUtils.close();
    }

    private JavaEntityClient getJavaEntityClient() {
        return new JavaEntityClient(
                _entityService,
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
    void testSuccessWithNoRetries() {
        JavaEntityClient client = getJavaEntityClient();

        Supplier<Object> mockSupplier = mock(Supplier.class);

        when(mockSupplier.get()).thenReturn(42);

        assertEquals(client.withRetry(mockSupplier, null), 42);
        verify(mockSupplier, times(1)).get();
        verify(MetricUtils.counter(any(), any()), times(0)).inc();
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

        assertEquals(client.withRetry(mockSupplier, null), 42);
        verify(mockSupplier, times(4)).get();
        verify(
                MetricUtils.counter(JavaEntityClient.class, "deleteEntityReferences_exception_IllegalStateException"),
                times(3)
        ).inc();
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

        assertThrows(IllegalStateException.class, () -> client.withRetry(mockSupplier, null));
        verify(mockSupplier, times(4)).get();
        verify(
                MetricUtils.counter(JavaEntityClient.class, "deleteEntityReferences_exception_IllegalStateException"),
                times(4)
        ).inc();
    }

    @Test
    void testThrowAfterNonRetryableException() {
        JavaEntityClient client = getJavaEntityClient();

        Supplier<Object> mockSupplier = mock(Supplier.class);

        when(mockSupplier.get())
                .thenThrow(new RequiredFieldNotPresentException("error"));

        assertThrows(RequiredFieldNotPresentException.class, () -> client.withRetry(mockSupplier, null));
        verify(mockSupplier, times(1)).get();
        verify(
                MetricUtils.counter(JavaEntityClient.class, "deleteEntityReferences_exception_RequiredFieldNotPresentException"),
                times(1)
        ).inc();
    }
}