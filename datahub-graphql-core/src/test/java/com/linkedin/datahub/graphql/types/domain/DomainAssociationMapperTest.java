package com.linkedin.datahub.graphql.types.domain;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.DomainAssociation;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.domain.DomainAssociationArray;
import com.linkedin.domain.Domains;
import java.net.URISyntaxException;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainAssociationMapperTest {

  private static final String TEST_DOMAIN_URN = "urn:li:domain:engineering";
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testuser";
  private static final Long TEST_TIMESTAMP = 1640995200000L;

  private Urn domainUrn;
  private Urn actorUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    domainUrn = Urn.createFromString(TEST_DOMAIN_URN);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapWithDomainAssociation() {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));
    com.linkedin.domain.DomainAssociation assoc = new com.linkedin.domain.DomainAssociation();
    assoc.setDomain(domainUrn);
    domains.setDomainAssociations(new DomainAssociationArray(assoc));

    DomainAssociation result = DomainAssociationMapper.map(null, domains, TEST_ENTITY_URN);

    assertNotNull(result);
    assertEquals(result.getDomain().getUrn(), TEST_DOMAIN_URN);
    assertEquals(result.getDomain().getType(), EntityType.DOMAIN);
    assertEquals(result.getAssociatedUrn(), TEST_ENTITY_URN);
    assertNull(result.getAttribution());
  }

  @Test
  public void testMapWithAttribution() {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));
    com.linkedin.domain.DomainAssociation assoc = new com.linkedin.domain.DomainAssociation();
    assoc.setDomain(domainUrn);
    MetadataAttribution attribution = new MetadataAttribution();
    attribution.setTime(TEST_TIMESTAMP);
    attribution.setActor(actorUrn);
    assoc.setAttribution(attribution);
    domains.setDomainAssociations(new DomainAssociationArray(assoc));

    DomainAssociation result = DomainAssociationMapper.map(null, domains, TEST_ENTITY_URN);

    assertNotNull(result);
    assertNotNull(result.getAttribution());
    assertEquals(result.getAttribution().getTime(), TEST_TIMESTAMP);
    assertEquals(result.getAttribution().getActor().getUrn(), TEST_ACTOR_URN);
  }

  @Test
  public void testMapReturnsNullWhenDomainAssociationsEmpty() {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray());
    domains.setDomainAssociations(new DomainAssociationArray());

    DomainAssociation result = DomainAssociationMapper.map(null, domains, TEST_ENTITY_URN);

    assertNull(result);
  }

  @Test
  public void testMapReturnsNullWhenDomainAssociationsNotSet() {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray());

    DomainAssociation result = DomainAssociationMapper.map(null, domains, TEST_ENTITY_URN);

    assertNull(result);
  }

  @Test
  public void testMapReturnsNullWhenCanViewFails() {
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));
    com.linkedin.domain.DomainAssociation assoc = new com.linkedin.domain.DomainAssociation();
    assoc.setDomain(domainUrn);
    domains.setDomainAssociations(new DomainAssociationArray(assoc));

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(false);

      DomainAssociation result =
          DomainAssociationMapper.map(mockQueryContext, domains, TEST_ENTITY_URN);

      assertNull(result);
    }
  }

  @Test
  public void testMapUsesFirstAssociation() throws URISyntaxException {
    Urn secondDomainUrn = Urn.createFromString("urn:li:domain:marketing");
    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn, secondDomainUrn));
    com.linkedin.domain.DomainAssociation assoc1 = new com.linkedin.domain.DomainAssociation();
    assoc1.setDomain(domainUrn);
    com.linkedin.domain.DomainAssociation assoc2 = new com.linkedin.domain.DomainAssociation();
    assoc2.setDomain(secondDomainUrn);
    domains.setDomainAssociations(new DomainAssociationArray(assoc1, assoc2));

    DomainAssociation result = DomainAssociationMapper.map(null, domains, TEST_ENTITY_URN);

    assertNotNull(result);
    assertEquals(result.getDomain().getUrn(), TEST_DOMAIN_URN);
  }
}
