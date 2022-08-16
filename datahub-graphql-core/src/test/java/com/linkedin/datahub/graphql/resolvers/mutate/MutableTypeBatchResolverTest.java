package com.linkedin.datahub.graphql.resolvers.mutate;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchDatasetUpdateInput;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetDeprecationUpdate;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.types.BatchMutableType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import com.linkedin.entity.Aspect;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletionException;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

public class MutableTypeBatchResolverTest {

    private static final String TEST_DATASET_1_URN = "urn:li:dataset:id-1";
    private static final String TEST_DATASET_2_URN = "urn:li:dataset:id-2";
    private static final boolean TEST_DATASET_1_IS_DEPRECATED = true;
    private static final boolean TEST_DATASET_2_IS_DEPRECATED = false;
    private static final String TEST_DATASET_1_DEPRECATION_NOTE = "Test Deprecation Note";
    private static final String TEST_DATASET_2_DEPRECATION_NOTE = "";
    private static final Deprecation TEST_DATASET_1_DEPRECATION;

    static {
        try {
            TEST_DATASET_1_DEPRECATION = new Deprecation()
                    .setDeprecated(TEST_DATASET_1_IS_DEPRECATED)
                    .setNote(TEST_DATASET_1_DEPRECATION_NOTE)
                    .setActor(Urn.createFromString("urn:li:corpuser:datahub"));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Deprecation TEST_DATASET_2_DEPRECATION;

    static {
        try {
            TEST_DATASET_2_DEPRECATION = new Deprecation()
                    .setDeprecated(TEST_DATASET_2_IS_DEPRECATED)
                    .setNote(TEST_DATASET_2_DEPRECATION_NOTE)
                    .setActor(Urn.createFromString("urn:li:corpuser:datahub"));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetSuccess() throws Exception {
        EntityClient mockClient = Mockito.mock(RestliEntityClient.class);
        BatchMutableType<DatasetUpdateInput, BatchDatasetUpdateInput, Dataset> batchMutableType = new DatasetType(mockClient);

        MutableTypeBatchResolver<DatasetUpdateInput, BatchDatasetUpdateInput, Dataset> resolver = new MutableTypeBatchResolver<>(batchMutableType);

        List<BatchDatasetUpdateInput> mockInputs = Arrays.asList(
            new BatchDatasetUpdateInput.Builder()
                    .setUrn(TEST_DATASET_1_URN)
                    .setUpdate(
                            new DatasetUpdateInput.Builder()
                                    .setDeprecation(
                                            new DatasetDeprecationUpdate.Builder()
                                                    .setDeprecated(TEST_DATASET_1_IS_DEPRECATED)
                                                    .setNote(TEST_DATASET_1_DEPRECATION_NOTE)
                                                    .build()
                                    )
                                    .build()
                    )
                    .build(),
            new BatchDatasetUpdateInput.Builder()
                    .setUrn(TEST_DATASET_2_URN)
                    .setUpdate(
                        new DatasetUpdateInput.Builder()
                                .setDeprecation(
                                        new DatasetDeprecationUpdate.Builder()
                                                .setDeprecated(TEST_DATASET_2_IS_DEPRECATED)
                                                .setNote(TEST_DATASET_2_DEPRECATION_NOTE)
                                                .build()
                                )
                                .build()
                    )
                    .build()
        );

        DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
        Mockito.when(mockEnv.getArgument("input")).thenReturn(mockInputs);
        QueryContext mockContext = getMockAllowContext();
        Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
        Authentication mockAuth = Mockito.mock(Authentication.class);
        Mockito.when(mockContext.getAuthentication()).thenReturn(mockAuth);
        Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));

        Urn datasetUrn1 = Urn.createFromString(TEST_DATASET_1_URN);
        Urn datasetUrn2 = Urn.createFromString(TEST_DATASET_2_URN);

        Mockito.when(mockClient.batchGetV2(Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(datasetUrn1, datasetUrn2))),
                Mockito.any(),
                Mockito.any(Authentication.class)))
                .thenReturn(ImmutableMap.of(
                        datasetUrn1,
                        new EntityResponse()
                                .setEntityName(Constants.DATASET_ENTITY_NAME)
                                .setUrn(datasetUrn1)
                                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                                        Constants.DATASET_DEPRECATION_ASPECT_NAME,
                                        new EnvelopedAspect().setValue(new Aspect(TEST_DATASET_1_DEPRECATION.data()))
                                ))),
                        datasetUrn2,
                        new EntityResponse()
                                .setEntityName(Constants.DATASET_ENTITY_NAME)
                                .setUrn(datasetUrn2)
                                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                                        Constants.DATASET_DEPRECATION_ASPECT_NAME,
                                        new EnvelopedAspect().setValue(new Aspect(TEST_DATASET_2_DEPRECATION.data()))
                                )))
                ));

        List<Dataset> result = resolver.get(mockEnv).join();

        ArgumentCaptor<Collection<MetadataChangeProposal>> changeProposalCaptor = ArgumentCaptor.forClass((Class) Collection.class);
        Mockito.verify(mockClient, Mockito.times(1)).batchIngestProposals(changeProposalCaptor.capture(), Mockito.any());
        Mockito.verify(mockClient, Mockito.times(1)).batchGetV2(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(datasetUrn1, datasetUrn2)),
                // Dataset aspects to fetch are private, but aren't important for this test
                Mockito.any(),
                Mockito.any(Authentication.class)
        );
        Collection<MetadataChangeProposal> changeProposals = changeProposalCaptor.getValue();

        assertEquals(changeProposals.size(), 2);
        assertEquals(result.size(), 2);
    }

    @Test
    public void testGetFailureUnauthorized() throws Exception {
        EntityClient mockClient = Mockito.mock(RestliEntityClient.class);
        BatchMutableType<DatasetUpdateInput, BatchDatasetUpdateInput, Dataset> batchMutableType = new DatasetType(mockClient);

        MutableTypeBatchResolver<DatasetUpdateInput, BatchDatasetUpdateInput, Dataset> resolver = new MutableTypeBatchResolver<>(batchMutableType);

        DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
        QueryContext mockContext = getMockDenyContext();
        Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

        assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    }
}
