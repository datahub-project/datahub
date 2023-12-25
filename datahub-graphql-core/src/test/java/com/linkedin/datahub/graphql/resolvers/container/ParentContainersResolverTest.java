package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CONTAINER_PROPERTIES_ASPECT_NAME;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ParentContainersResolverTest {
  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Dataset datasetEntity = new Dataset();
    datasetEntity.setUrn(datasetUrn.toString());
    datasetEntity.setType(EntityType.DATASET);
    Mockito.when(mockEnv.getSource()).thenReturn(datasetEntity);

    final Container parentContainer1 =
        new Container().setContainer(Urn.createFromString("urn:li:container:test-container"));
    final Container parentContainer2 =
        new Container().setContainer(Urn.createFromString("urn:li:container:test-container2"));

    Map<String, EnvelopedAspect> datasetAspects = new HashMap<>();
    datasetAspects.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(parentContainer1.data())));

    Map<String, EnvelopedAspect> parentContainer1Aspects = new HashMap<>();
    parentContainer1Aspects.put(
        CONTAINER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new ContainerProperties().setName("test_schema").data())));
    parentContainer1Aspects.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(parentContainer2.data())));

    Map<String, EnvelopedAspect> parentContainer2Aspects = new HashMap<>();
    parentContainer2Aspects.put(
        CONTAINER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new ContainerProperties().setName("test_database").data())));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(datasetUrn.getEntityType()),
                Mockito.eq(datasetUrn),
                Mockito.eq(Collections.singleton(CONTAINER_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(datasetAspects)));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(parentContainer1.getContainer().getEntityType()),
                Mockito.eq(parentContainer1.getContainer()),
                Mockito.eq(null),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(CONTAINER_ENTITY_NAME)
                .setUrn(parentContainer1.getContainer())
                .setAspects(new EnvelopedAspectMap(parentContainer1Aspects)));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(parentContainer1.getContainer().getEntityType()),
                Mockito.eq(parentContainer1.getContainer()),
                Mockito.eq(Collections.singleton(CONTAINER_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(parentContainer1Aspects)));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(parentContainer2.getContainer().getEntityType()),
                Mockito.eq(parentContainer2.getContainer()),
                Mockito.eq(null),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(CONTAINER_ENTITY_NAME)
                .setUrn(parentContainer2.getContainer())
                .setAspects(new EnvelopedAspectMap(parentContainer2Aspects)));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(parentContainer2.getContainer().getEntityType()),
                Mockito.eq(parentContainer2.getContainer()),
                Mockito.eq(Collections.singleton(CONTAINER_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse().setAspects(new EnvelopedAspectMap(parentContainer2Aspects)));

    ParentContainersResolver resolver = new ParentContainersResolver(mockClient);
    ParentContainersResult result = resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(5))
        .getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertEquals(result.getCount(), 2);
    assertEquals(
        result.getContainers().get(0).getUrn(), parentContainer1.getContainer().toString());
    assertEquals(
        result.getContainers().get(1).getUrn(), parentContainer2.getContainer().toString());
  }
}
