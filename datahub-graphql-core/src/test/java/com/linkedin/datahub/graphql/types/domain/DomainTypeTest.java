package com.linkedin.datahub.graphql.types.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DomainTypeTest {

  private static final String TEST_DOMAIN_1_URN = "urn:li:domain:id-1";
  private static final DomainKey TEST_DOMAIN_1_KEY = new DomainKey().setId("id-1");
  private static final DomainProperties TEST_DOMAIN_1_PROPERTIES =
      new DomainProperties().setDescription("test description").setName("Test Domain");
  private static final Ownership TEST_DOMAIN_1_OWNERSHIP =
      new Ownership()
          .setOwners(
              new OwnerArray(
                  ImmutableList.of(
                      new Owner()
                          .setType(OwnershipType.DATAOWNER)
                          .setOwner(Urn.createFromTuple("corpuser", "test")))));
  private static final InstitutionalMemory TEST_DOMAIN_1_INSTITUTIONAL_MEMORY =
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

  private static final String TEST_DOMAIN_2_URN = "urn:li:domain:id-2";

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn domainUrn1 = Urn.createFromString(TEST_DOMAIN_1_URN);
    Urn domainUrn2 = Urn.createFromString(TEST_DOMAIN_2_URN);

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(domainUrn1, domainUrn2))),
                Mockito.eq(DomainType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                domainUrn1,
                new EntityResponse()
                    .setEntityName(Constants.DOMAIN_ENTITY_NAME)
                    .setUrn(domainUrn1)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DOMAIN_KEY_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DOMAIN_1_KEY.data())),
                                Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DOMAIN_1_PROPERTIES.data())),
                                Constants.OWNERSHIP_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DOMAIN_1_OWNERSHIP.data())),
                                Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(
                                        new Aspect(TEST_DOMAIN_1_INSTITUTIONAL_MEMORY.data())))))));

    DomainType type = new DomainType(client);

    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<Domain>> result =
        type.batchLoad(ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(domainUrn1, domainUrn2)),
            Mockito.eq(DomainType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    Domain domain1 = result.get(0).getData();
    assertEquals(domain1.getUrn(), TEST_DOMAIN_1_URN);
    assertEquals(domain1.getId(), "id-1");
    assertEquals(domain1.getType(), EntityType.DOMAIN);
    assertEquals(domain1.getOwnership().getOwners().size(), 1);
    assertEquals(domain1.getProperties().getDescription(), "test description");
    assertEquals(domain1.getProperties().getName(), "Test Domain");
    assertEquals(domain1.getInstitutionalMemory().getElements().size(), 1);

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    DomainType type = new DomainType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN), context));
  }
}
