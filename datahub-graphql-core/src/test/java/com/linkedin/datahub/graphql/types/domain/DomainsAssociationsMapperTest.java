package com.linkedin.datahub.graphql.types.domain;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.DomainsAssociations;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.domain.Domains;
import org.testng.annotations.Test;

public class DomainsAssociationsMapperTest {

  private static final String TEST_DOMAIN_URN_1 = "urn:li:domain:engineering";
  private static final String TEST_DOMAIN_URN_2 = "urn:li:domain:marketing";
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_test.users,PROD)";

  @Test
  public void testMapWithValidDomains() throws Exception {
    QueryContext mockContext = TestUtils.getMockAllowContext();
    Domains input = createTestDomains();
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN);

    DomainsAssociations result = DomainsAssociationsMapper.map(mockContext, input, entityUrn);

    assertNotNull(result);
    assertEquals(result.getDomains().size(), 2);

    assertEquals(result.getDomains().get(0).getDomain().getUrn(), TEST_DOMAIN_URN_1);
    assertEquals(result.getDomains().get(0).getDomain().getType(), EntityType.DOMAIN);
    assertEquals(result.getDomains().get(0).getAssociatedUrn(), TEST_ENTITY_URN);

    assertEquals(result.getDomains().get(1).getDomain().getUrn(), TEST_DOMAIN_URN_2);
    assertEquals(result.getDomains().get(1).getDomain().getType(), EntityType.DOMAIN);
    assertEquals(result.getDomains().get(1).getAssociatedUrn(), TEST_ENTITY_URN);
  }

  @Test
  public void testMapWithNullContext() throws Exception {
    Domains input = createTestDomains();
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN);

    DomainsAssociations result = DomainsAssociationsMapper.map(null, input, entityUrn);

    assertNotNull(result);
    assertEquals(result.getDomains().size(), 2);
  }

  @Test
  public void testMapWithEmptyDomains() throws Exception {
    QueryContext mockContext = TestUtils.getMockAllowContext();
    Domains input = new Domains();
    input.setDomains(new UrnArray());
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN);

    DomainsAssociations result = DomainsAssociationsMapper.map(mockContext, input, entityUrn);

    assertNotNull(result);
    assertTrue(result.getDomains().isEmpty());
  }

  @Test
  public void testMapWithAuthorizationFiltering() throws Exception {
    QueryContext mockDenyContext = TestUtils.getMockDenyContext();
    Domains input = createTestDomains();
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN);

    DomainsAssociations result = DomainsAssociationsMapper.map(mockDenyContext, input, entityUrn);

    assertNotNull(result);
    assertTrue(result.getDomains().size() <= input.getDomains().size());
  }

  @Test
  public void testStaticMapMethod() throws Exception {
    QueryContext mockContext = TestUtils.getMockAllowContext();
    Domains input = createTestDomains();
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN);

    DomainsAssociations result = DomainsAssociationsMapper.map(mockContext, input, entityUrn);

    assertNotNull(result);
    assertEquals(result.getDomains().size(), 2);
  }

  @Test
  public void testMapWithSingleDomain() throws Exception {
    QueryContext mockContext = TestUtils.getMockAllowContext();
    Domains input = new Domains();
    UrnArray domainUrns = new UrnArray();
    domainUrns.add(UrnUtils.getUrn(TEST_DOMAIN_URN_1));
    input.setDomains(domainUrns);
    Urn entityUrn = UrnUtils.getUrn(TEST_ENTITY_URN);

    DomainsAssociations result = DomainsAssociationsMapper.map(mockContext, input, entityUrn);

    assertNotNull(result);
    assertEquals(result.getDomains().size(), 1);
    assertEquals(result.getDomains().get(0).getDomain().getUrn(), TEST_DOMAIN_URN_1);
  }

  private Domains createTestDomains() {
    Domains domains = new Domains();
    UrnArray domainUrns = new UrnArray();
    domainUrns.add(UrnUtils.getUrn(TEST_DOMAIN_URN_1));
    domainUrns.add(UrnUtils.getUrn(TEST_DOMAIN_URN_2));
    domains.setDomains(domainUrns);
    return domains;
  }
}
