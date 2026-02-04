package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests to verify that transitive permissions work for entities in child domains. When a dataset is
 * in a child domain, policies on parent domains should automatically apply due to the hierarchical
 * domain resolution.
 */
public class DomainFieldResolverProviderTransitiveTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,test.dataset,PROD)");
  private static final Urn CHILD_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:finance-us");
  private static final Urn PARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:finance");
  private static final Urn ROOT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:root");

  private SystemEntityClient mockClient;
  private DomainFieldResolverProvider resolver;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockClient = mock(SystemEntityClient.class);
    resolver = new DomainFieldResolverProvider(mockClient);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testDatasetInChildDomainIncludesParentDomains() throws Exception {
    Domains domainsAspect = new Domains();
    domainsAspect.setDomains(new UrnArray(ImmutableList.of(CHILD_DOMAIN_URN)));

    EntityResponse datasetResponse = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        DOMAINS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(domainsAspect.data())));
    datasetResponse.setAspects(aspectMap);

    DomainProperties childDomainProps = new DomainProperties();
    childDomainProps.setParentDomain(PARENT_DOMAIN_URN);

    EntityResponse childDomainResponse = new EntityResponse();
    EnvelopedAspectMap childDomainAspectMap = new EnvelopedAspectMap();
    childDomainAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childDomainProps.data())));
    childDomainResponse.setAspects(childDomainAspectMap);

    EntityResponse parentDomainResponse = new EntityResponse();
    parentDomainResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(datasetResponse);

    when(mockClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Collections.singleton(CHILD_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(CHILD_DOMAIN_URN, childDomainResponse));

    when(mockClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Collections.singleton(PARENT_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(PARENT_DOMAIN_URN, parentDomainResponse));

    EntitySpec datasetSpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());
    Set<String> domainValues =
        resolver.getFieldResolver(opContext, datasetSpec).getFieldValuesFuture().join().getValues();

    assertEquals(domainValues.size(), 2);
    assertTrue(domainValues.contains(CHILD_DOMAIN_URN.toString()));
    assertTrue(domainValues.contains(PARENT_DOMAIN_URN.toString()));
  }

  @Test
  public void testDatasetInGrandchildDomainIncludesAllAncestors() throws Exception {
    Urn grandchildDomainUrn = UrnUtils.getUrn("urn:li:domain:finance-us-california");

    Domains domainsAspect = new Domains();
    domainsAspect.setDomains(new UrnArray(ImmutableList.of(grandchildDomainUrn)));

    EntityResponse datasetResponse = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        DOMAINS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(domainsAspect.data())));
    datasetResponse.setAspects(aspectMap);

    DomainProperties grandchildProps = new DomainProperties();
    grandchildProps.setParentDomain(CHILD_DOMAIN_URN);

    EntityResponse grandchildResponse = new EntityResponse();
    EnvelopedAspectMap grandchildAspectMap = new EnvelopedAspectMap();
    grandchildAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(grandchildProps.data())));
    grandchildResponse.setAspects(grandchildAspectMap);

    DomainProperties childProps = new DomainProperties();
    childProps.setParentDomain(PARENT_DOMAIN_URN);

    EntityResponse childResponse = new EntityResponse();
    EnvelopedAspectMap childAspectMap = new EnvelopedAspectMap();
    childAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childProps.data())));
    childResponse.setAspects(childAspectMap);

    DomainProperties parentProps = new DomainProperties();
    parentProps.setParentDomain(ROOT_DOMAIN_URN);

    EntityResponse parentResponse = new EntityResponse();
    EnvelopedAspectMap parentAspectMap = new EnvelopedAspectMap();
    parentAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentProps.data())));
    parentResponse.setAspects(parentAspectMap);

    EntityResponse rootResponse = new EntityResponse();
    rootResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(datasetResponse);

    when(mockClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Collections.singleton(grandchildDomainUrn)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(grandchildDomainUrn, grandchildResponse));

    when(mockClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Collections.singleton(CHILD_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(CHILD_DOMAIN_URN, childResponse));

    when(mockClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Collections.singleton(PARENT_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(PARENT_DOMAIN_URN, parentResponse));

    when(mockClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Collections.singleton(ROOT_DOMAIN_URN)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(ROOT_DOMAIN_URN, rootResponse));

    EntitySpec datasetSpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());
    Set<String> domainValues =
        resolver.getFieldResolver(opContext, datasetSpec).getFieldValuesFuture().join().getValues();

    assertEquals(domainValues.size(), 4);
    assertTrue(domainValues.contains(grandchildDomainUrn.toString()));
    assertTrue(domainValues.contains(CHILD_DOMAIN_URN.toString()));
    assertTrue(domainValues.contains(PARENT_DOMAIN_URN.toString()));
    assertTrue(domainValues.contains(ROOT_DOMAIN_URN.toString()));
  }

  @Test
  public void testDatasetWithNoDomainReturnsEmpty() throws Exception {
    EntityResponse datasetResponse = new EntityResponse();
    datasetResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_URN),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(datasetResponse);

    EntitySpec datasetSpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());
    Set<String> domainValues =
        resolver.getFieldResolver(opContext, datasetSpec).getFieldValuesFuture().join().getValues();

    assertTrue(domainValues.isEmpty());
  }
}
