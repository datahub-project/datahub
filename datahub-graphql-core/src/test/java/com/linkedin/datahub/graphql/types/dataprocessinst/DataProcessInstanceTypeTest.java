package com.linkedin.datahub.graphql.types.dataprocessinst;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.StringMap;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataProcessInstanceTypeTest {

    private static final String TEST_INSTANCE_URN = "urn:li:dataProcessInstance:(test-workflow,test-instance-1)";

    @Test
    public void testBatchLoad() throws Exception {
        EntityClient mockClient = Mockito.mock(EntityClient.class);
        FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
        Mockito.when(mockFeatureFlags.isDataProcessInstanceEntityEnabled()).thenReturn(true);

        DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);

        List<DataFetcherResult<DataProcessInstance>> result =
            type.batchLoad(ImmutableList.of(TEST_INSTANCE_URN), getMockAllowContext());

        assertEquals(result.size(), 1);
    }

    @Test
    public void testBatchLoadFeatureFlagDisabled() throws Exception {
        EntityClient mockClient = Mockito.mock(EntityClient.class);
        FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
        Mockito.when(mockFeatureFlags.isDataProcessInstanceEntityEnabled()).thenReturn(false);

        DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);

        List<DataFetcherResult<DataProcessInstance>> result =
            type.batchLoad(ImmutableList.of(TEST_INSTANCE_URN), getMockAllowContext());

        assertEquals(result.size(), 0);

        Mockito.verify(mockClient, Mockito.never())
            .batchGetV2(
                any(),
                Mockito.anyString(),
                Mockito.anySet(),
                Mockito.anySet());
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testBatchLoadClientException() throws Exception {
        EntityClient mockClient = Mockito.mock(EntityClient.class);
        FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
        Mockito.when(mockFeatureFlags.isDataProcessInstanceEntityEnabled()).thenReturn(true);

        Mockito.doThrow(RemoteInvocationException.class)
            .when(mockClient)
            .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());

        DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);
        type.batchLoad(ImmutableList.of(TEST_INSTANCE_URN), getMockAllowContext());
    }

    @Test
    public void testGetType() {
        EntityClient mockClient = Mockito.mock(EntityClient.class);
        FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
        DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);

        assertEquals(type.type(), EntityType.DATA_PROCESS_INSTANCE);
    }

    @Test
    public void testObjectClass() {
        EntityClient mockClient = Mockito.mock(EntityClient.class);
        FeatureFlags mockFeatureFlags = Mockito.mock(FeatureFlags.class);
        DataProcessInstanceType type = new DataProcessInstanceType(mockClient, mockFeatureFlags);

        assertEquals(type.objectClass(), DataProcessInstance.class);
    }
}