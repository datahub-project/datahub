package com.linkedin.metadata.aspect.utils;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.*;
import org.testng.annotations.Test;

/**
 * Unit tests for DomainExtractionUtils.
 *
 * <p>Tests extraction of domain information from MetadataChangeProposals for domain-based
 * authorization.
 */
public class DomainExtractionUtilsTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
  private static final Urn DOMAIN_URN_1 = UrnUtils.getUrn("urn:li:domain:domain1");
  private static final Urn DOMAIN_URN_2 = UrnUtils.getUrn("urn:li:domain:domain2");
  private static final Urn EXECUTION_REQUEST_URN =
      UrnUtils.getUrn("urn:li:dataHubExecutionRequest:request1");

  private MetadataChangeProposal createDomainMCP(Urn entityUrn, Urn... domainUrns) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(entityUrn);
    mcp.setAspectName(DOMAINS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(Arrays.asList(domainUrns)));
    mcp.setAspect(GenericRecordUtils.serializeAspect(domains));

    return mcp;
  }

  @Test
  public void testExtractNewDomainsFromMCPs_SingleDomain() {
    MetadataChangeProposal mcp = createDomainMCP(DATASET_URN, DOMAIN_URN_1);

    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromMCPs(Collections.singletonList(mcp));

    assertEquals(result.size(), 1);
    assertTrue(result.containsKey(DATASET_URN));
    assertEquals(result.get(DATASET_URN), Collections.singleton(DOMAIN_URN_1));
  }

  @Test
  public void testExtractNewDomainsFromMCPs_MultipleDomains() {
    MetadataChangeProposal mcp = createDomainMCP(DATASET_URN, DOMAIN_URN_1, DOMAIN_URN_2);

    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromMCPs(Collections.singletonList(mcp));

    assertEquals(result.size(), 1);
    assertTrue(result.containsKey(DATASET_URN));
    assertEquals(result.get(DATASET_URN), new HashSet<>(Arrays.asList(DOMAIN_URN_1, DOMAIN_URN_2)));
  }

  @Test
  public void testExtractNewDomainsFromMCPs_EmptyDomains() {
    MetadataChangeProposal mcp = createDomainMCP(DATASET_URN); // No domains

    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromMCPs(Collections.singletonList(mcp));

    // Empty domains should not be included in result
    assertEquals(result.size(), 0);
  }

  @Test
  public void testExtractNewDomainsFromMCPs_NonDomainsAspect() {
    // Create MCP with non-domains aspect
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(DATASET_URN);
    mcp.setAspectName("datasetProperties");
    mcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromMCPs(Collections.singletonList(mcp));

    // Should not extract anything from non-domains aspects
    assertEquals(result.size(), 0);
  }

  @Test
  public void testExtractNewDomainsFromMCPs_ExecutionRequestEntity() {
    MetadataChangeProposal mcp = createDomainMCP(EXECUTION_REQUEST_URN, DOMAIN_URN_1);

    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromMCPs(Collections.singletonList(mcp));

    // Execution requests should be filtered out
    assertEquals(result.size(), 0);
  }

  @Test
  public void testExtractNewDomainsFromMCPs_MultipleMCPs() {
    Urn dataset2Urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test2,PROD)");
    MetadataChangeProposal mcp1 = createDomainMCP(DATASET_URN, DOMAIN_URN_1);
    MetadataChangeProposal mcp2 = createDomainMCP(dataset2Urn, DOMAIN_URN_2);

    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromMCPs(Arrays.asList(mcp1, mcp2));

    assertEquals(result.size(), 2);
    assertTrue(result.containsKey(DATASET_URN));
    assertTrue(result.containsKey(dataset2Urn));
    assertEquals(result.get(DATASET_URN), Collections.singleton(DOMAIN_URN_1));
    assertEquals(result.get(dataset2Urn), Collections.singleton(DOMAIN_URN_2));
  }

  @Test
  public void testExtractNewDomainsFromMCPs_MixedAspects() {
    MetadataChangeProposal domainMcp = createDomainMCP(DATASET_URN, DOMAIN_URN_1);

    MetadataChangeProposal propertiesMcp = new MetadataChangeProposal();
    propertiesMcp.setEntityUrn(DATASET_URN);
    propertiesMcp.setAspectName("datasetProperties");
    propertiesMcp.setChangeType(ChangeType.UPSERT);

    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromMCPs(Arrays.asList(domainMcp, propertiesMcp));

    // Should only extract from domains aspect
    assertEquals(result.size(), 1);
    assertTrue(result.containsKey(DATASET_URN));
    assertEquals(result.get(DATASET_URN), Collections.singleton(DOMAIN_URN_1));
  }

  @Test
  public void testExtractNewDomainsFromMCPs_EmptyList() {
    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromMCPs(Collections.emptyList());

    assertEquals(result.size(), 0);
  }

  @Test
  public void testExtractNewDomainsFromEntities_EmptyList() {
    Map<Urn, Set<Urn>> result =
        DomainExtractionUtils.extractNewDomainsFromEntities(Collections.emptyList());

    assertEquals(result.size(), 0);
  }
}
