package com.linkedin.datahub.graphql.types.container;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ContainerKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ContainerTypeTest {

  private static final String TEST_CONTAINER_1_URN = "urn:li:container:guid-1";
  private static final ContainerKey TEST_CONTAINER_1_KEY = new ContainerKey().setGuid("guid-1");
  private static final ContainerProperties TEST_CONTAINER_1_PROPERTIES =
      new ContainerProperties().setDescription("test description").setName("Test Container");
  private static final EditableContainerProperties TEST_CONTAINER_1_EDITABLE_PROPERTIES =
      new EditableContainerProperties().setDescription("test editable description");
  private static final Ownership TEST_CONTAINER_1_OWNERSHIP =
      new Ownership()
          .setOwners(
              new OwnerArray(
                  ImmutableList.of(
                      new Owner()
                          .setType(OwnershipType.DATAOWNER)
                          .setOwner(Urn.createFromTuple("corpuser", "test")))));
  private static final InstitutionalMemory TEST_CONTAINER_1_INSTITUTIONAL_MEMORY =
      new InstitutionalMemory()
          .setElements(
              new InstitutionalMemoryMetadataArray(
                  ImmutableList.of(
                      new InstitutionalMemoryMetadata()
                          .setUrl(new Url("https://www.test.com"))
                          .setDescription("test description")
                          .setCreateStamp(
                              new AuditStamp()
                                  .setTime(0L)
                                  .setActor(Urn.createFromTuple("corpuser", "test"))))));
  private static final DataPlatformInstance TEST_CONTAINER_1_DATA_PLATFORM_INSTANCE =
      new DataPlatformInstance().setPlatform(Urn.createFromTuple("dataPlatform", "mysql"));
  private static final Status TEST_CONTAINER_1_STATUS = new Status().setRemoved(false);
  private static final SubTypes TEST_CONTAINER_1_SUB_TYPES =
      new SubTypes().setTypeNames(new StringArray(ImmutableList.of("Database")));
  private static final GlobalTags TEST_CONTAINER_1_TAGS =
      new GlobalTags()
          .setTags(
              new TagAssociationArray(
                  ImmutableList.of(new TagAssociation().setTag(new TagUrn("test")))));
  private static final GlossaryTerms TEST_CONTAINER_1_GLOSSARY_TERMS =
      new GlossaryTerms()
          .setTerms(
              new GlossaryTermAssociationArray(
                  ImmutableList.of(
                      new GlossaryTermAssociation().setUrn(new GlossaryTermUrn("term")))));
  private static final com.linkedin.container.Container TEST_CONTAINER_1_CONTAINER =
      new com.linkedin.container.Container()
          .setContainer(Urn.createFromTuple(Constants.CONTAINER_ENTITY_NAME, "parent-container"));

  private static final String TEST_CONTAINER_2_URN = "urn:li:container:guid-2";

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = mock(EntityClient.class);

    Urn containerUrn1 = Urn.createFromString(TEST_CONTAINER_1_URN);
    Urn containerUrn2 = Urn.createFromString(TEST_CONTAINER_2_URN);

    Map<String, EnvelopedAspect> container1Aspects = new HashMap<>();
    container1Aspects.put(
        Constants.CONTAINER_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_KEY.data())));
    container1Aspects.put(
        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_DATA_PLATFORM_INSTANCE.data())));
    container1Aspects.put(
        Constants.CONTAINER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_PROPERTIES.data())));
    container1Aspects.put(
        Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_EDITABLE_PROPERTIES.data())));
    container1Aspects.put(
        Constants.OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_OWNERSHIP.data())));
    container1Aspects.put(
        Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_INSTITUTIONAL_MEMORY.data())));
    container1Aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_SUB_TYPES.data())));
    container1Aspects.put(
        Constants.STATUS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_STATUS.data())));
    container1Aspects.put(
        Constants.GLOBAL_TAGS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_TAGS.data())));
    container1Aspects.put(
        Constants.GLOSSARY_TERMS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_GLOSSARY_TERMS.data())));
    container1Aspects.put(
        Constants.CONTAINER_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_CONTAINER_1_CONTAINER.data())));
    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.CONTAINER_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(containerUrn1, containerUrn2))),
                Mockito.eq(ContainerType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                containerUrn1,
                new EntityResponse()
                    .setEntityName(Constants.CONTAINER_ENTITY_NAME)
                    .setUrn(containerUrn1)
                    .setAspects(new EnvelopedAspectMap(container1Aspects))));

    ContainerType type = new ContainerType(client);

    QueryContext mockContext = mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    List<DataFetcherResult<Container>> result =
        type.batchLoad(ImmutableList.of(TEST_CONTAINER_1_URN, TEST_CONTAINER_2_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.CONTAINER_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(containerUrn1, containerUrn2)),
            Mockito.eq(ContainerType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    Container container1 = result.get(0).getData();
    assertEquals(container1.getUrn(), TEST_CONTAINER_1_URN);
    assertEquals(container1.getType(), EntityType.CONTAINER);
    assertEquals(container1.getOwnership().getOwners().size(), 1);
    assertEquals(container1.getProperties().getDescription(), "test description");
    assertEquals(container1.getProperties().getName(), "Test Container");
    assertEquals(container1.getInstitutionalMemory().getElements().size(), 1);
    assertEquals(
        container1.getSubTypes().getTypeNames().get(0),
        TEST_CONTAINER_1_SUB_TYPES.getTypeNames().get(0));
    assertEquals(
        container1.getEditableProperties().getDescription(),
        TEST_CONTAINER_1_EDITABLE_PROPERTIES.getDescription());
    assertEquals(
        container1.getGlossaryTerms().getTerms().get(0).getTerm().getUrn(),
        TEST_CONTAINER_1_GLOSSARY_TERMS.getTerms().get(0).getUrn().toString());
    assertEquals(
        container1.getTags().getTags().get(0).getTag().getUrn(),
        TEST_CONTAINER_1_TAGS.getTags().get(0).getTag().toString());
    assertEquals(
        container1.getContainer().getUrn(), TEST_CONTAINER_1_CONTAINER.getContainer().toString());

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    ContainerType type = new ContainerType(mockClient);

    // Execute Batch load
    QueryContext context = mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () ->
            type.batchLoad(ImmutableList.of(TEST_CONTAINER_1_URN, TEST_CONTAINER_2_URN), context));
  }
}
