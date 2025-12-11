package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParentDomainFieldResolverProviderTest {

  private static final Urn CHILD_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:finance-us");
  private static final Urn PARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:finance");
  private static final Urn GRANDPARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:root");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,test.dataset,PROD)");

  private SystemEntityClient mockClient;
  private ParentDomainFieldResolverProvider resolver;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockClient = mock(SystemEntityClient.class);
    resolver = new ParentDomainFieldResolverProvider(mockClient);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testGetFieldTypes() {
    assertEquals(resolver.getFieldTypes().size(), 1);
    assertEquals(resolver.getFieldTypes().get(0), EntityFieldType.PARENT_DOMAIN);
  }

  @Test
  public void testEmptyEntitySpec() throws Exception {
    EntitySpec emptySpec = new EntitySpec(DOMAIN_ENTITY_NAME, "");
    Set<String> result =
        resolver.getFieldResolver(opContext, emptySpec).getFieldValuesFuture().join().getValues();
    assertTrue(result.isEmpty());
  }

  @Test
  public void testNonDomainEntity() throws Exception {
    EntitySpec datasetSpec = new EntitySpec(DATASET_ENTITY_NAME, DATASET_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, datasetSpec).getFieldValuesFuture().join().getValues();
    assertTrue(result.isEmpty());
  }

  @Test
  public void testDomainWithNoParent() throws Exception {
    EntityResponse response = new EntityResponse();
    response.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(PARENT_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(response);

    EntitySpec domainSpec = new EntitySpec(DOMAIN_ENTITY_NAME, PARENT_DOMAIN_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, domainSpec).getFieldValuesFuture().join().getValues();

    assertTrue(result.isEmpty());
  }

  @Test
  public void testDomainWithSingleParent() throws Exception {
    DomainProperties childProps = new DomainProperties();
    childProps.setParentDomain(PARENT_DOMAIN_URN);

    EntityResponse childResponse = new EntityResponse();
    EnvelopedAspectMap childAspectMap = new EnvelopedAspectMap();
    childAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childProps.data())));
    childResponse.setAspects(childAspectMap);

    EntityResponse parentResponse = new EntityResponse();
    parentResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(CHILD_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(childResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(PARENT_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(parentResponse);

    EntitySpec domainSpec = new EntitySpec(DOMAIN_ENTITY_NAME, CHILD_DOMAIN_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, domainSpec).getFieldValuesFuture().join().getValues();

    assertEquals(result.size(), 1);
    assertTrue(result.contains(PARENT_DOMAIN_URN.toString()));
  }

  @Test
  public void testDomainWithMultipleParents() throws Exception {
    DomainProperties childProps = new DomainProperties();
    childProps.setParentDomain(PARENT_DOMAIN_URN);

    EntityResponse childResponse = new EntityResponse();
    EnvelopedAspectMap childAspectMap = new EnvelopedAspectMap();
    childAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childProps.data())));
    childResponse.setAspects(childAspectMap);

    DomainProperties parentProps = new DomainProperties();
    parentProps.setParentDomain(GRANDPARENT_DOMAIN_URN);

    EntityResponse parentResponse = new EntityResponse();
    EnvelopedAspectMap parentAspectMap = new EnvelopedAspectMap();
    parentAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentProps.data())));
    parentResponse.setAspects(parentAspectMap);

    EntityResponse grandparentResponse = new EntityResponse();
    grandparentResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(CHILD_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(childResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(PARENT_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(parentResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(GRANDPARENT_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(grandparentResponse);

    EntitySpec domainSpec = new EntitySpec(DOMAIN_ENTITY_NAME, CHILD_DOMAIN_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, domainSpec).getFieldValuesFuture().join().getValues();

    assertEquals(result.size(), 2);
    assertTrue(result.contains(PARENT_DOMAIN_URN.toString()));
    assertTrue(result.contains(GRANDPARENT_DOMAIN_URN.toString()));
  }

  @Test
  public void testCycleDetection() throws Exception {
    DomainProperties childProps = new DomainProperties();
    childProps.setParentDomain(PARENT_DOMAIN_URN);

    EntityResponse childResponse = new EntityResponse();
    EnvelopedAspectMap childAspectMap = new EnvelopedAspectMap();
    childAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childProps.data())));
    childResponse.setAspects(childAspectMap);

    DomainProperties parentProps = new DomainProperties();
    parentProps.setParentDomain(CHILD_DOMAIN_URN);

    EntityResponse parentResponse = new EntityResponse();
    EnvelopedAspectMap parentAspectMap = new EnvelopedAspectMap();
    parentAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentProps.data())));
    parentResponse.setAspects(parentAspectMap);

    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(CHILD_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(childResponse);

    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(PARENT_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(parentResponse);

    EntitySpec domainSpec = new EntitySpec(DOMAIN_ENTITY_NAME, CHILD_DOMAIN_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, domainSpec).getFieldValuesFuture().join().getValues();

    assertEquals(result.size(), 2);
    assertTrue(result.contains(PARENT_DOMAIN_URN.toString()));
    assertTrue(result.contains(CHILD_DOMAIN_URN.toString()));
  }

  @Test
  public void testErrorHandling() throws Exception {
    when(mockClient.getV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(CHILD_DOMAIN_URN),
            eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenThrow(new RuntimeException("Test exception"));

    EntitySpec domainSpec = new EntitySpec(DOMAIN_ENTITY_NAME, CHILD_DOMAIN_URN.toString());
    Set<String> result =
        resolver.getFieldResolver(opContext, domainSpec).getFieldValuesFuture().join().getValues();

    assertTrue(result.isEmpty());
  }
}
