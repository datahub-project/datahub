package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.generated.ActionRequestType.*;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_PENDING;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import java.util.Collections;
import org.testng.annotations.Test;

public class ProposalUtilsTest {
  private static final String TEST_USER_URN = "urn:li:corpuser:testUser";
  private static final String DEFAULT_RESOURCE =
      "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)";
  private static final String DEFAULT_RESOURCE_2 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset2,PROD)";

  private Boolean isFilterAppplied(Filter capturedFilter, String key, String value) {
    boolean foundResourceCondition = false;

    for (ConjunctiveCriterion cc : capturedFilter.getOr()) {
      if (cc.getAnd() != null) {
        for (Criterion c : cc.getAnd()) {
          if (key.equals(c.getField()) && c.getValues() != null && c.getValues().contains(value)) {
            foundResourceCondition = true;
            break;
          }
        }
      }
    }

    return foundResourceCondition;
  }

  @Test
  public void testCreateFilter() throws Exception {
    // Test case 1: With assignee filters (user)
    Urn userUrn = UrnUtils.getUrn(TEST_USER_URN);
    Filter filter1 =
        ProposalUtils.createFilter(
            userUrn,
            null,
            null,
            ActionRequestType.TAG_ASSOCIATION,
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.COMPLETED,
            UrnUtils.getUrn(DEFAULT_RESOURCE),
            12345L,
            56789L);
    assertNotNull(filter1);
    assertNotNull(filter1.getOr());
    assertEquals(filter1.getOr().size(), 1);

    assertTrue(isFilterAppplied(filter1, "assignedUsers.keyword", TEST_USER_URN));
    assertTrue(isFilterAppplied(filter1, "type", TAG_ASSOCIATION.toString()));
    assertTrue(isFilterAppplied(filter1, "status", ACTION_REQUEST_STATUS_COMPLETE));
    assertTrue(isFilterAppplied(filter1, "resource", DEFAULT_RESOURCE));
    assertTrue(isFilterAppplied(filter1, "lastModified", "12345"));
    assertTrue(isFilterAppplied(filter1, "lastModified", "56789"));

    // Test case 2: With assignee filters (group)
    Urn groupUrn = UrnUtils.getUrn("urn:li:corpGroup:group1");
    Filter filter2 =
        ProposalUtils.createFilter(
            null,
            Collections.singletonList(groupUrn),
            null,
            ActionRequestType.TERM_ASSOCIATION,
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING,
            null,
            null,
            null);
    assertNotNull(filter2);
    assertNotNull(filter2.getOr());
    assertEquals(filter2.getOr().size(), 1);

    assertTrue(isFilterAppplied(filter2, "assignedGroups.keyword", groupUrn.toString()));
    assertTrue(isFilterAppplied(filter2, "type", TERM_ASSOCIATION.toString()));
    assertTrue(isFilterAppplied(filter2, "status", ACTION_REQUEST_STATUS_PENDING));

    // Test case 3: With non-assignee filters only
    Filter filter3 =
        ProposalUtils.createFilter(
            null,
            null,
            null,
            ActionRequestType.DOMAIN_ASSOCIATION,
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.COMPLETED,
            UrnUtils.getUrn(DEFAULT_RESOURCE_2),
            45678L,
            null);
    assertNotNull(filter3);
    assertNotNull(filter3.getOr());
    assertEquals(filter3.getOr().size(), 1);

    assertTrue(isFilterAppplied(filter3, "type", DOMAIN_ASSOCIATION.toString()));
    assertTrue(isFilterAppplied(filter3, "status", ACTION_REQUEST_STATUS_COMPLETE));
    assertTrue(isFilterAppplied(filter3, "resource", DEFAULT_RESOURCE_2));
    assertTrue(isFilterAppplied(filter3, "lastModified", "45678"));

    // Test case 4: No filters
    Filter filter4 = ProposalUtils.createFilter(null, null, null, null, null, null, null, null);
    assertNotNull(filter4);
    assertNotNull(filter4.getOr());
    assertEquals(filter4.getOr().size(), 0);
  }
}
