package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.util.ServiceTestUtils.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DomainServiceTest {

  private static final Urn TEST_DOMAIN_URN_1 = UrnUtils.getUrn("urn:li:domain:test");
  private static final Urn TEST_DOMAIN_URN_2 = UrnUtils.getUrn("urn:li:domain:test2");

  private static final Urn TEST_ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)");
  private static final Urn TEST_ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,test1,PROD)");

  @Test
  private void testSetDomainExistingDomain() throws Exception {
    Domains existingDomains = new Domains();
    existingDomains.setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1)));
    OpenApiClient mockClient = createMockDomainsClient(existingDomains);

    final DomainService service = createDomainsService(mockClient);

    Urn newDomainUrn = UrnUtils.getUrn("urn:li:domain:newDomain");
    List<MetadataChangeProposal> events =
        service.buildSetDomainProposals(
            newDomainUrn,
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(ImmutableList.of(newDomainUrn))));

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2, new Domains().setDomains(new UrnArray(ImmutableList.of(newDomainUrn))));
  }

  private DomainService createDomainsService(OpenApiClient client) {
    return new DomainService(
        Mockito.mock(EntityClient.class), Mockito.mock(Authentication.class), client);
  }

  @Test
  private void testSetDomainNoExistingDomain() throws Exception {
    OpenApiClient mockClient = createMockDomainsClient(null);

    final DomainService service = createDomainsService(mockClient);

    Urn newDomainUrn = UrnUtils.getUrn("urn:li:domain:newDomain");
    List<MetadataChangeProposal> events =
        service.buildSetDomainProposals(
            newDomainUrn,
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(ImmutableList.of(newDomainUrn))));

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
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
    OpenApiClient mockClient = createMockDomainsClient(existingDomains);

    final DomainService service = createDomainsService(mockClient);

    List<MetadataChangeProposal> events =
        service.buildUnsetDomainProposals(
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(Collections.emptyList())));

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2, new Domains().setDomains(new UrnArray(Collections.emptyList())));
  }

  @Test
  private void testUnsetDomainNoExistingDomain() throws Exception {
    OpenApiClient mockClient = createMockDomainsClient(null);

    final DomainService service =
        new DomainService(
            Mockito.mock(EntityClient.class), Mockito.mock(Authentication.class), mockClient);

    List<MetadataChangeProposal> events =
        service.buildUnsetDomainProposals(
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)));

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(Collections.emptyList())));

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
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
    OpenApiClient mockClient = createMockDomainsClient(existingDomains);

    final DomainService service = createDomainsService(mockClient);

    List<MetadataChangeProposal> events =
        service.buildAddDomainsProposals(
            ImmutableList.of(TEST_DOMAIN_URN_2),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1,
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1, TEST_DOMAIN_URN_2))));

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
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
    OpenApiClient mockClient = createMockDomainsClient(null);

    final DomainService service = createDomainsService(mockClient);

    List<MetadataChangeProposal> events =
        service.buildAddDomainsProposals(
            ImmutableList.of(TEST_DOMAIN_URN_1),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1,
        new Domains().setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1))));

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
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
    OpenApiClient mockClient = createMockDomainsClient(existingDomains);

    final DomainService service =
        new DomainService(
            Mockito.mock(EntityClient.class), Mockito.mock(Authentication.class), mockClient);

    List<MetadataChangeProposal> events =
        service.buildRemoveDomainsProposals(
            ImmutableList.of(TEST_DOMAIN_URN_2),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1,
        new Domains().setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1))));

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
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
    OpenApiClient mockClient = createMockDomainsClient(null);

    final DomainService service =
        new DomainService(
            Mockito.mock(EntityClient.class), Mockito.mock(Authentication.class), mockClient);

    List<MetadataChangeProposal> events =
        service.buildRemoveDomainsProposals(
            ImmutableList.of(TEST_DOMAIN_URN_2),
            ImmutableList.of(
                new ResourceReference(TEST_ENTITY_URN_1, null, null),
                new ResourceReference(TEST_ENTITY_URN_2, null, null)),
            mockAuthentication());

    MetadataChangeProposal event1 = events.get(0);
    Assert.assertEquals(event1.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event1.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect1 =
        GenericRecordUtils.deserializeAspect(
            event1.getAspect().getValue(), event1.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect1, new Domains().setDomains(new UrnArray(Collections.emptyList())));

    MetadataChangeProposal event2 = events.get(0);
    Assert.assertEquals(event2.getAspectName(), Constants.DOMAINS_ASPECT_NAME);
    Assert.assertEquals(event2.getEntityType(), Constants.DATASET_ENTITY_NAME);
    RecordTemplate domainsAspect2 =
        GenericRecordUtils.deserializeAspect(
            event2.getAspect().getValue(), event2.getAspect().getContentType(), Domains.class);
    Assert.assertEquals(
        domainsAspect2, new Domains().setDomains(new UrnArray(Collections.emptyList())));
  }
}
