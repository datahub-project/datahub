package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ParentDomainsResult;
import com.linkedin.domain.DomainProperties;
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

public class ParentDomainsResolverTest {
  @Test
  public void testGetSuccessForDomain() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Urn domainUrn = Urn.createFromString("urn:li:domain:00005397daf94708a8822b8106cfd451");
    Domain domainEntity = new Domain();
    domainEntity.setUrn(domainUrn.toString());
    domainEntity.setType(EntityType.DOMAIN);
    Mockito.when(mockEnv.getSource()).thenReturn(domainEntity);

    final DomainProperties parentDomain1 =
        new DomainProperties()
            .setParentDomain(Urn.createFromString("urn:li:domain:11115397daf94708a8822b8106cfd451"))
            .setName("test def");
    final DomainProperties parentDomain2 =
        new DomainProperties()
            .setParentDomain(Urn.createFromString("urn:li:domain:22225397daf94708a8822b8106cfd451"))
            .setName("test def 2");

    Map<String, EnvelopedAspect> domainAspects = new HashMap<>();
    domainAspects.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentDomain1.data())));

    Map<String, EnvelopedAspect> parentDomain1Aspects = new HashMap<>();
    parentDomain1Aspects.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new Aspect(
                    new DomainProperties()
                        .setName("domain parent 1")
                        .setParentDomain(parentDomain2.getParentDomain())
                        .data())));

    Map<String, EnvelopedAspect> parentDomain2Aspects = new HashMap<>();
    parentDomain2Aspects.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new DomainProperties().setName("domain parent 2").data())));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(domainUrn.getEntityType()),
                Mockito.eq(domainUrn),
                Mockito.eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(domainAspects)));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(parentDomain1.getParentDomain().getEntityType()),
                Mockito.eq(parentDomain1.getParentDomain()),
                Mockito.eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentDomain1Aspects)));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(parentDomain2.getParentDomain().getEntityType()),
                Mockito.eq(parentDomain2.getParentDomain()),
                Mockito.eq(Collections.singleton(DOMAIN_PROPERTIES_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentDomain2Aspects)));

    ParentDomainsResolver resolver = new ParentDomainsResolver(mockClient);
    ParentDomainsResult result = resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(3))
        .getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertEquals(result.getCount(), 2);
    assertEquals(result.getDomains().get(0).getUrn(), parentDomain1.getParentDomain().toString());
    assertEquals(result.getDomains().get(1).getUrn(), parentDomain2.getParentDomain().toString());
  }
}
