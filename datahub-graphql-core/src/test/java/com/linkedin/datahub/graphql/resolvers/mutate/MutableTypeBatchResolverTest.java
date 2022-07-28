package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.BatchMutableType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.concurrent.CompletionException;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

public class MutableTypeBatchResolverTest {

    @Test
    public void testGetFailureUnauthorized() throws Exception {
        EntityClient mockClient = Mockito.mock(RestliEntityClient.class);
        BatchMutableType mutableType = new DatasetType(mockClient);

        MutableTypeBatchResolver resolver = new MutableTypeBatchResolver(mutableType);

        DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
        QueryContext mockContext = getMockDenyContext();
        Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

        assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    }
}
