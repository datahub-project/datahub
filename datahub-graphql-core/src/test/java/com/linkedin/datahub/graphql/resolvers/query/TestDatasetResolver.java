package com.linkedin.datahub.graphql.resolvers.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.loaders.DatasetLoader;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.PlatformNativeType;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class TestDatasetResolver {

    private static final DatasetResolver RESOLVER = new DatasetResolver();

    @Test
    public void testResolverUrnNotFound() throws Exception {
        DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);

        QueryContext context = mock(QueryContext.class);
        when(context.isAuthenticated()).thenReturn(true);

        DataLoader mockLoader = mock(DataLoader.class);
        when(mockLoader.load("urn:li:dataset:missingDataset")).thenReturn(
                CompletableFuture.completedFuture(null));

        when(env.getContext()).thenReturn(context);
        when(env.getArgument("urn")).thenReturn("urn:li:dataset:missingDataset");
        when(env.getDataLoader(DatasetLoader.NAME)).thenReturn(mockLoader);

        assertEquals(RESOLVER.get(env).get(), null);
    }

    @Test
    public void testResolverSuccess() throws Exception {
        DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);

        QueryContext context = mock(QueryContext.class);
        when(context.isAuthenticated()).thenReturn(true);

        DataLoader mockLoader = mock(DataLoader.class);
        when(mockLoader.load("urn:li:dataset:testDataset")).thenReturn(
                CompletableFuture.completedFuture(testDataset()));

        when(env.getContext()).thenReturn(context);
        when(env.getArgument("urn")).thenReturn("urn:li:dataset:testDataset");
        when(env.getDataLoader(DatasetLoader.NAME)).thenReturn(mockLoader);

        Dataset expectedDataset = testDataset();
        com.linkedin.datahub.graphql.generated.Dataset actualDataset = RESOLVER.get(env).get();

        assertEquals(actualDataset.getUrn(), expectedDataset.getUrn().toString());
        assertEquals(actualDataset.getDescription(), expectedDataset.getDescription());
        assertEquals(actualDataset.getName(), expectedDataset.getName());
        assertEquals(actualDataset.getTags(), expectedDataset.getTags());
        assertEquals(actualDataset.getPlatform(), expectedDataset.getPlatform().toString());
        assertEquals(actualDataset.getUri(), null);
        assertEquals(actualDataset.getOrigin(), com.linkedin.datahub.graphql.generated.FabricType.valueOf(expectedDataset.getOrigin().toString()));
        assertEquals(actualDataset.getPlatformNativeType(),
                com.linkedin.datahub.graphql.generated.PlatformNativeType.valueOf(expectedDataset.getPlatformNativeType().toString()));

        assertEquals(actualDataset.getCreated().getActor(), expectedDataset.getSchemaMetadata().getCreated().getActor().toString());
        assertEquals(actualDataset.getCreated().getTime(), expectedDataset.getSchemaMetadata().getCreated().getTime());

        assertEquals(actualDataset.getLastModified().getActor(), expectedDataset.getSchemaMetadata().getLastModified().getActor().toString());
        assertEquals(actualDataset.getLastModified().getTime(), expectedDataset.getSchemaMetadata().getLastModified().getTime());

        assertEquals(actualDataset.getOwnership().getLastModified().getActor(), expectedDataset.getOwnership().getLastModified().getActor().toString());
        assertEquals(actualDataset.getOwnership().getLastModified().getTime(), expectedDataset.getOwnership().getLastModified().getTime());

        Owner expectedOwner = expectedDataset.getOwnership().getOwners().get(0);
        com.linkedin.datahub.graphql.generated.Owner actualOwner = actualDataset.getOwnership().getOwners().get(0);

        assertEquals(actualOwner.getType(),
                com.linkedin.datahub.graphql.generated.OwnershipType.valueOf(expectedOwner.getType().toString()));
        assertEquals(actualOwner.getSource().getType(),
                com.linkedin.datahub.graphql.generated.OwnershipSourceType.valueOf(expectedOwner.getSource().getType().toString()));
        assertEquals(actualOwner.getSource().getUrl(), expectedOwner.getSource().getUrl());

        assertEquals(actualOwner.getOwner().getUrn(), expectedOwner.getOwner().toString());
    }

    private Dataset testDataset() throws URISyntaxException {
        Dataset validDataset = new com.linkedin.dataset.Dataset()
                .setUrn(DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,TestDataset,PROD)"))
                .setDescription("Test Dataset Description")
                .setName("TestDataset")
                .setOrigin(FabricType.PROD)
                .setPlatform(DataPlatformUrn.createFromString("urn:li:dataPlatform:hive"))
                .setPlatformNativeType(PlatformNativeType.TABLE)
                .setTags(new StringArray("tag1", "tag2"))
                .setSchemaMetadata(new SchemaMetadata()
                        .setCreated(new AuditStamp().setActor(Urn.createFromString("urn:li:corpuser:1")).setTime(0L))
                        .setLastModified(new AuditStamp().setActor(Urn.createFromString("urn:li:corpuser:2")).setTime(1L))
                )
                .setOwnership(new Ownership()
                        .setLastModified(new AuditStamp().setActor(Urn.createFromString("urn:li:corpuser:3")).setTime(2L))
                        .setOwners(new OwnerArray(ImmutableList.of(
                                new Owner()
                                    .setOwner(Urn.createFromString("urn:li:corpuser:test"))
                                    .setType(OwnershipType.DATAOWNER)
                                    .setSource(new OwnershipSource()
                                            .setType(OwnershipSourceType.FILE_SYSTEM)
                                            .setUrl("www.datahub.test")
                                    )
                        )))
                );
        return validDataset;
    }
}
