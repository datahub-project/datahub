package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.types.MutableType;
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

    private static final DatasetUpdateInput[] TEST_INPUTS = new DatasetUpdateInput[]{
        new DatasetUpdateInput(), new DatasetUpdateInput()
    };

    @Test
    public void testGetUnsupportedOperation() throws Exception {
        EntityClient mockClient = Mockito.mock(RestliEntityClient.class);
        MutableType mutableType = new DatasetType(mockClient);

        MutableTypeBatchResolver resolver = new MutableTypeBatchResolver(mutableType);

        DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
        Mockito.when(mockEnv.getArgument(Mockito.eq("inputs"))).thenReturn(TEST_INPUTS);
        QueryContext mockContext = getMockAllowContext();
        Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

        assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    }

    @Test
    public void testGetFailureUnauthorized() throws Exception {
        EntityClient mockClient = Mockito.mock(RestliEntityClient.class);
        MutableType mutableType = new DatasetType(mockClient);

        MutableTypeBatchResolver resolver = new MutableTypeBatchResolver(mutableType);

        DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
        QueryContext mockContext = getMockDenyContext();
        Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

        assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    }
}
