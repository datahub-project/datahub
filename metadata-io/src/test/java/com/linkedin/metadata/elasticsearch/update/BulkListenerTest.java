package com.linkedin.metadata.elasticsearch.update;

import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class BulkListenerTest {

    @Test
    public void testConstructor() {
        BulkListener test = BulkListener.getInstance();
        assertNotNull(test);
        assertEquals(test, BulkListener.getInstance());
        assertNotEquals(test, BulkListener.getInstance(WriteRequest.RefreshPolicy.IMMEDIATE));
    }

    @Test
    public void testDefaultPolicy() {
        BulkListener test = BulkListener.getInstance();

        BulkRequest mockRequest1 = Mockito.mock(BulkRequest.class);
        test.beforeBulk(0L, mockRequest1);
        verify(mockRequest1, times(0)).setRefreshPolicy(any(WriteRequest.RefreshPolicy.class));

        BulkRequest mockRequest2 = Mockito.mock(BulkRequest.class);
        test = BulkListener.getInstance(WriteRequest.RefreshPolicy.IMMEDIATE);
        test.beforeBulk(0L, mockRequest2);
        verify(mockRequest2, times(1)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
    }
}
