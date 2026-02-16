package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<DomainFieldResolverProvider> {

  private SystemEntityClient mockEntityClient;
  private OperationContext mockOpContext;
  private DomainFieldResolverProvider provider;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    mockOpContext = mock(OperationContext.class);
    provider = new DomainFieldResolverProvider(mockEntityClient);
  }

  @Override
  protected DomainFieldResolverProvider buildFieldResolverProvider() {
    return new DomainFieldResolverProvider(mock(SystemEntityClient.class));
  }

  @Test
  public void testGetFieldTypes() {
    List<EntityFieldType> fieldTypes = provider.getFieldTypes();
    assertEquals(fieldTypes.size(), 1);
    assertEquals(fieldTypes.get(0), EntityFieldType.DOMAIN);
  }

  @Test
  public void testGetDomainsWithProposedAspect() throws Exception {
    // Test the new functionality for proposed domains during authorization
    Urn domainUrn1 = UrnUtils.getUrn("urn:li:domain:engineering");
    Urn domainUrn2 = UrnUtils.getUrn("urn:li:domain:platform");
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    Domains proposedDomains = new Domains();
    proposedDomains.setDomains(new UrnArray(domainUrn1, domainUrn2));

    Map<String, com.linkedin.data.template.RecordTemplate> proposedAspects = new HashMap<>();
    proposedAspects.put(DOMAINS_ASPECT_NAME, proposedDomains);

    EntitySpec entitySpec = new EntitySpec("dataset", datasetUrn.toString(), proposedAspects);

    // Mock no parent domains
    when(mockEntityClient.batchGetV2(
            eq(mockOpContext),
            eq(DOMAIN_ENTITY_NAME),
            any(),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Collections.emptyMap());

    FieldResolver resolver = provider.getFieldResolver(mockOpContext, entitySpec);
    FieldResolver.FieldValue fieldValue = resolver.getFieldValuesFuture().get();

    assertNotNull(fieldValue);
    assertEquals(fieldValue.getValues().size(), 2);
    assertTrue(fieldValue.getValues().contains(domainUrn1.toString()));
    assertTrue(fieldValue.getValues().contains(domainUrn2.toString()));
  }

  @Test
  public void testGetDomainsWhenEntityIsDomain() throws Exception {
    // Test special case where entity is a domain itself
    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:engineering");

    EntitySpec entitySpec = new EntitySpec(DOMAIN_ENTITY_NAME, domainUrn.toString());

    FieldResolver resolver = provider.getFieldResolver(mockOpContext, entitySpec);
    FieldResolver.FieldValue fieldValue = resolver.getFieldValuesFuture().get();

    assertNotNull(fieldValue);
    assertEquals(fieldValue.getValues().size(), 1);
    assertTrue(fieldValue.getValues().contains(domainUrn.toString()));

    // Should not call entity client for domain entity
    verify(mockEntityClient, never()).getV2(any(), any(), any(), any());
  }

  @Test
  public void testGetDomainsWithExistingAspect() throws Exception {
    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:engineering");
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));

    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(domains.data()));
    aspectMap.put(DOMAINS_ASPECT_NAME, envelopedAspect);
    response.setAspects(aspectMap);

    when(mockEntityClient.getV2(
            eq(mockOpContext),
            eq("dataset"),
            eq(datasetUrn),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(response);

    // Mock no parent domains
    when(mockEntityClient.batchGetV2(
            eq(mockOpContext),
            eq(DOMAIN_ENTITY_NAME),
            any(),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Collections.emptyMap());

    EntitySpec entitySpec = new EntitySpec("dataset", datasetUrn.toString());

    FieldResolver resolver = provider.getFieldResolver(mockOpContext, entitySpec);
    FieldResolver.FieldValue fieldValue = resolver.getFieldValuesFuture().get();

    assertNotNull(fieldValue);
    assertEquals(fieldValue.getValues().size(), 1);
    assertTrue(fieldValue.getValues().contains(domainUrn.toString()));
  }

  @Test
  public void testGetDomainsWithParentDomains() throws Exception {
    Urn childDomainUrn = UrnUtils.getUrn("urn:li:domain:engineering");
    Urn parentDomainUrn = UrnUtils.getUrn("urn:li:domain:tech");
    Urn grandparentDomainUrn = UrnUtils.getUrn("urn:li:domain:company");
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(childDomainUrn));

    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(domains.data()));
    aspectMap.put(DOMAINS_ASPECT_NAME, envelopedAspect);
    response.setAspects(aspectMap);

    when(mockEntityClient.getV2(
            eq(mockOpContext),
            eq("dataset"),
            eq(datasetUrn),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(response);

    // Mock parent domain hierarchy
    DomainProperties childProps = new DomainProperties();
    childProps.setParentDomain(parentDomainUrn);
    EntityResponse childResponse = new EntityResponse();
    EnvelopedAspectMap childAspectMap = new EnvelopedAspectMap();
    EnvelopedAspect childEnvelopedAspect = new EnvelopedAspect();
    childEnvelopedAspect.setValue(new Aspect(childProps.data()));
    childAspectMap.put(DOMAIN_PROPERTIES_ASPECT_NAME, childEnvelopedAspect);
    childResponse.setAspects(childAspectMap);

    DomainProperties parentProps = new DomainProperties();
    parentProps.setParentDomain(grandparentDomainUrn);
    EntityResponse parentResponse = new EntityResponse();
    EnvelopedAspectMap parentAspectMap = new EnvelopedAspectMap();
    EnvelopedAspect parentEnvelopedAspect = new EnvelopedAspect();
    parentEnvelopedAspect.setValue(new Aspect(parentProps.data()));
    parentAspectMap.put(DOMAIN_PROPERTIES_ASPECT_NAME, parentEnvelopedAspect);
    parentResponse.setAspects(parentAspectMap);

    // First call returns parent
    when(mockEntityClient.batchGetV2(
            eq(mockOpContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(childDomainUrn)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(childDomainUrn, childResponse));

    // Second call returns grandparent
    when(mockEntityClient.batchGetV2(
            eq(mockOpContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(parentDomainUrn)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(parentDomainUrn, parentResponse));

    // Third call returns no more parents
    when(mockEntityClient.batchGetV2(
            eq(mockOpContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(grandparentDomainUrn)),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Collections.emptyMap());

    EntitySpec entitySpec = new EntitySpec("dataset", datasetUrn.toString());

    FieldResolver resolver = provider.getFieldResolver(mockOpContext, entitySpec);
    FieldResolver.FieldValue fieldValue = resolver.getFieldValuesFuture().get();

    assertNotNull(fieldValue);
    assertEquals(fieldValue.getValues().size(), 3);
    assertTrue(fieldValue.getValues().contains(childDomainUrn.toString()));
    assertTrue(fieldValue.getValues().contains(parentDomainUrn.toString()));
    assertTrue(fieldValue.getValues().contains(grandparentDomainUrn.toString()));
  }

  @Test
  public void testGetDomainsNoDomainsAspect() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    EntityResponse response = new EntityResponse();
    response.setAspects(new EnvelopedAspectMap());

    when(mockEntityClient.getV2(
            eq(mockOpContext),
            eq("dataset"),
            eq(datasetUrn),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(response);

    EntitySpec entitySpec = new EntitySpec("dataset", datasetUrn.toString());

    FieldResolver resolver = provider.getFieldResolver(mockOpContext, entitySpec);
    FieldResolver.FieldValue fieldValue = resolver.getFieldValuesFuture().get();

    assertNotNull(fieldValue);
    assertTrue(fieldValue.getValues().isEmpty());
  }

  @Test
  public void testGetDomainsEntityClientReturnsNull() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockEntityClient.getV2(
            eq(mockOpContext),
            eq("dataset"),
            eq(datasetUrn),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(null);

    EntitySpec entitySpec = new EntitySpec("dataset", datasetUrn.toString());

    FieldResolver resolver = provider.getFieldResolver(mockOpContext, entitySpec);
    FieldResolver.FieldValue fieldValue = resolver.getFieldValuesFuture().get();

    assertNotNull(fieldValue);
    assertTrue(fieldValue.getValues().isEmpty());
  }

  @Test
  public void testGetDomainsEntityClientThrowsException() throws Exception {
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockEntityClient.getV2(
            eq(mockOpContext),
            eq("dataset"),
            eq(datasetUrn),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenThrow(new RuntimeException("Connection error"));

    EntitySpec entitySpec = new EntitySpec("dataset", datasetUrn.toString());

    FieldResolver resolver = provider.getFieldResolver(mockOpContext, entitySpec);
    FieldResolver.FieldValue fieldValue = resolver.getFieldValuesFuture().get();

    assertNotNull(fieldValue);
    assertTrue(fieldValue.getValues().isEmpty());
  }

  @Test
  public void testGetDomainsBatchGetParentDomainsThrowsException() throws Exception {
    Urn domainUrn = UrnUtils.getUrn("urn:li:domain:engineering");
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));

    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(domains.data()));
    aspectMap.put(DOMAINS_ASPECT_NAME, envelopedAspect);
    response.setAspects(aspectMap);

    when(mockEntityClient.getV2(
            eq(mockOpContext),
            eq("dataset"),
            eq(datasetUrn),
            eq(Collections.singleton(DOMAINS_ASPECT_NAME))))
        .thenReturn(response);

    // Mock exception during parent domain batch fetch
    when(mockEntityClient.batchGetV2(
            eq(mockOpContext),
            eq(DOMAIN_ENTITY_NAME),
            any(),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenThrow(new RuntimeException("Batch fetch error"));

    EntitySpec entitySpec = new EntitySpec("dataset", datasetUrn.toString());

    FieldResolver resolver = provider.getFieldResolver(mockOpContext, entitySpec);
    FieldResolver.FieldValue fieldValue = resolver.getFieldValuesFuture().get();

    // Should still return the domain even if parent fetch fails
    assertNotNull(fieldValue);
    assertEquals(fieldValue.getValues().size(), 1);
    assertTrue(fieldValue.getValues().contains(domainUrn.toString()));
  }
}
