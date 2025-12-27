package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.organization.OrganizationProperties;
import graphql.schema.DataFetchingEnvironment;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadOrganizationPropertiesResolverTest {

  private EntityClient _entityClient;
  private LoadOrganizationPropertiesResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _resolver = new LoadOrganizationPropertiesResolver(_entityClient);
  }

  @Test
  public void testLoadOrganizationPropertiesSuccess() throws Exception {
    Organization org1 = new Organization();
    org1.setUrn("urn:li:organization:org1");
    Organization org2 = new Organization();
    org2.setUrn("urn:li:organization:org2");

    List<Organization> organizations = new ArrayList<>();
    organizations.add(org1);
    organizations.add(org2);

    Entity mockEntity = Mockito.mock(Entity.class);
    Method getOrganizationsMethod = Entity.class.getMethod("getOrganizations");
    Mockito.when(getOrganizationsMethod.invoke(mockEntity)).thenReturn(organizations);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(mockEntity);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    OrganizationProperties props1 = new OrganizationProperties();
    props1.setName("Organization 1");
    props1.setDescription("Description 1");

    OrganizationProperties props2 = new OrganizationProperties();
    props2.setName("Organization 2");

    EntityResponse response1 = new EntityResponse();
    EnvelopedAspectMap aspects1 = new EnvelopedAspectMap();
    EnvelopedAspect aspect1 = new EnvelopedAspect();
    aspect1.setValue(
        new com.linkedin.entity.Aspect(GenericRecordUtils.serializeAspect(props1).data()));
    aspects1.put(ORGANIZATION_PROPERTIES_ASPECT_NAME, aspect1);
    response1.setAspects(aspects1);

    EntityResponse response2 = new EntityResponse();
    EnvelopedAspectMap aspects2 = new EnvelopedAspectMap();
    EnvelopedAspect aspect2 = new EnvelopedAspect();
    aspect2.setValue(
        new com.linkedin.entity.Aspect(GenericRecordUtils.serializeAspect(props2).data()));
    aspects2.put(ORGANIZATION_PROPERTIES_ASPECT_NAME, aspect2);
    response2.setAspects(aspects2);

    Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    batchResponse.put(Urn.createFromString("urn:li:organization:org1"), response1);
    batchResponse.put(Urn.createFromString("urn:li:organization:org2"), response2);

    Mockito.when(_entityClient.batchGetV2(any(), anyString(), any(), any()))
        .thenReturn(batchResponse);

    List<Organization> result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertNotNull(result.get(0).getProperties());
    assertEquals(result.get(0).getProperties().getName(), "Organization 1");
    assertEquals(result.get(0).getProperties().getDescription(), "Description 1");
    assertNotNull(result.get(1).getProperties());
    assertEquals(result.get(1).getProperties().getName(), "Organization 2");
  }

  @Test
  public void testLoadOrganizationPropertiesEmpty() throws Exception {
    Entity mockEntity = Mockito.mock(Entity.class);
    Method getOrganizationsMethod = Entity.class.getMethod("getOrganizations");
    Mockito.when(getOrganizationsMethod.invoke(mockEntity)).thenReturn(Collections.emptyList());

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(mockEntity);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    List<Organization> result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.size(), 0);
  }

  @Test
  public void testLoadOrganizationPropertiesNull() throws Exception {
    Entity mockEntity = Mockito.mock(Entity.class);
    Method getOrganizationsMethod = Entity.class.getMethod("getOrganizations");
    Mockito.when(getOrganizationsMethod.invoke(mockEntity)).thenReturn(null);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(mockEntity);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    List<Organization> result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.size(), 0);
  }

  @Test
  public void testLoadOrganizationPropertiesWithInvalidUrn() throws Exception {
    Organization org1 = new Organization();
    org1.setUrn("invalid-urn");

    List<Organization> organizations = new ArrayList<>();
    organizations.add(org1);

    Entity mockEntity = Mockito.mock(Entity.class);
    Method getOrganizationsMethod = Entity.class.getMethod("getOrganizations");
    Mockito.when(getOrganizationsMethod.invoke(mockEntity)).thenReturn(organizations);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(mockEntity);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    Mockito.when(_entityClient.batchGetV2(any(), anyString(), any(), any()))
        .thenReturn(Collections.emptyMap());

    List<Organization> result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.size(), 1);
  }
}
