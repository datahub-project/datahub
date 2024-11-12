package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.service.util.ServiceTestUtils.createMockDomainsClient;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DomainServiceTest {

  private static final Urn TEST_DOMAIN_URN_1 = UrnUtils.getUrn("urn:li:domain:test");
  private static final Urn TEST_DOMAIN_URN_2 = UrnUtils.getUrn("urn:li:domain:test2");

  private static final Urn TEST_ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final Urn TEST_ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");
  private static AspectRetriever mockAspectRetriever;
  private static OperationContext opContext;

  @BeforeClass
  public void init() {
    mockAspectRetriever = mock(AspectRetriever.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);
  }

  @Test
  private void testSetDomainExistingDomain() throws Exception {
    Domains existingDomains = new Domains();
    existingDomains.setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1)));
    setMockAspectRetriever(existingDomains);
    OpenApiClient mockOpenAPIClient = createMockDomainsClient(existingDomains);

    final DomainService service = createDomainsService(mockOpenAPIClient);

    Urn newDomainUrn = UrnUtils.getUrn("urn:li:domain:newDomain");
    List<MetadataChangeProposal> events =
        service.buildSetDomainProposals(
            newDomainUrn,
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            null);

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(ImmutableList.of(newDomainUrn))));

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2, new Domains().setDomains(new UrnArray(ImmutableList.of(newDomainUrn))));
  }

  private DomainService createDomainsService(OpenApiClient mockOpenAPIClient) {
    return new DomainService(
        mock(SystemEntityClient.class), mockOpenAPIClient, opContext.getObjectMapper());
  }

  @Test
  private void testSetDomainNoExistingDomain() throws Exception {
    setMockAspectRetriever(null);
    OpenApiClient mockOpenAPIClient = createMockDomainsClient(null);

    final DomainService service = createDomainsService(mockOpenAPIClient);

    Urn newDomainUrn = UrnUtils.getUrn("urn:li:domain:newDomain");
    List<MetadataChangeProposal> events =
        service.buildSetDomainProposals(
            newDomainUrn,
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            null);

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(ImmutableList.of(newDomainUrn))));

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2, new Domains().setDomains(new UrnArray(ImmutableList.of(newDomainUrn))));
  }

  @Test
  private void testUnsetDomainExistingDomain() throws Exception {
    Domains existingDomains = new Domains();
    existingDomains.setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1)));
    setMockAspectRetriever(existingDomains);
    OpenApiClient mockOpenAPIClient = createMockDomainsClient(existingDomains);

    final DomainService service = createDomainsService(mockOpenAPIClient);

    List<MetadataChangeProposal> events =
        service.buildUnsetDomainProposals(
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(Collections.emptyList())));

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2, new Domains().setDomains(new UrnArray(Collections.emptyList())));
  }

  @Test
  private void testUnsetDomainNoExistingDomain() throws Exception {
    setMockAspectRetriever(null);
    OpenApiClient mockOpenAPIClient = createMockDomainsClient(null);

    final DomainService service = createDomainsService(mockOpenAPIClient);

    List<MetadataChangeProposal> events =
        service.buildUnsetDomainProposals(
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(Collections.emptyList())));

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2, new Domains().setDomains(new UrnArray(Collections.emptyList())));
  }

  @Test
  private void testAddDomainsExistingDomain() throws Exception {
    Domains existingDomains = new Domains();
    existingDomains.setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1)));
    setMockAspectRetriever(existingDomains);
    OpenApiClient mockOpenAPIClient = createMockDomainsClient(existingDomains);

    final DomainService service = createDomainsService(mockOpenAPIClient);

    List<MetadataChangeProposal> events =
        service.buildAddDomainsProposals(
            opContext,
            ImmutableList.of(TEST_DOMAIN_URN_2),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1,
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1, TEST_DOMAIN_URN_2))));

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2,
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1, TEST_DOMAIN_URN_2))));
  }

  @Test
  private void testAddDomainsNoExistingDomain() throws Exception {
    setMockAspectRetriever(null);
    OpenApiClient mockOpenAPIClient = createMockDomainsClient(null);

    final DomainService service = createDomainsService(mockOpenAPIClient);

    List<MetadataChangeProposal> events =
        service.buildAddDomainsProposals(
            opContext,
            ImmutableList.of(TEST_DOMAIN_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1,
        new Domains().setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1))));

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2,
        new Domains().setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1))));
  }

  @Test
  private void testRemoveDomainsExistingDomain() throws Exception {
    Domains existingDomains = new Domains();
    existingDomains.setDomains(
        new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1, TEST_DOMAIN_URN_2)));
    setMockAspectRetriever(existingDomains);
    OpenApiClient mockOpenAPIClient = createMockDomainsClient(existingDomains);

    final DomainService service = createDomainsService(mockOpenAPIClient);

    List<MetadataChangeProposal> events =
        service.buildRemoveDomainsProposals(
            opContext,
            ImmutableList.of(TEST_DOMAIN_URN_2),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1,
        new Domains().setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1))));

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2,
        new Domains().setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1))));
  }

  @Test
  private void testRemoveDomainsNoExistingDomain() throws Exception {
    setMockAspectRetriever(null);
    OpenApiClient mockOpenAPIClient = createMockDomainsClient(null);

    final DomainService service = createDomainsService(mockOpenAPIClient);

    List<MetadataChangeProposal> events =
        service.buildRemoveDomainsProposals(
            opContext,
            ImmutableList.of(TEST_DOMAIN_URN_2),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(Collections.emptyList())));

    MetadataChangeProposal event2 = events.get(1);
    Assert.assertEquals(event2.getAspectName(), DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2, new Domains().setDomains(new UrnArray(Collections.emptyList())));
  }

  private static void setMockAspectRetriever(@Nullable Domains existingDomains) {
    reset(mockAspectRetriever);

    if (existingDomains != null) {
      Mockito.when(
              mockAspectRetriever.getLatestAspectObjects(
                  eq(Set.of(TEST_ENTITY_URN_1, TEST_ENTITY_URN_2)),
                  eq(Set.of(DOMAINS_ASPECT_NAME))))
          .thenReturn(
              ImmutableMap.of(
                  TEST_ENTITY_URN_1,
                  Map.of(DOMAINS_ASPECT_NAME, new Aspect(existingDomains.data())),
                  TEST_ENTITY_URN_2,
                  Map.of(DOMAINS_ASPECT_NAME, new Aspect(existingDomains.data()))));
    } else {
      Mockito.when(mockAspectRetriever.getLatestAspectObjects(anySet(), anySet()))
          .thenReturn(Collections.emptyMap());
    }
  }
}
