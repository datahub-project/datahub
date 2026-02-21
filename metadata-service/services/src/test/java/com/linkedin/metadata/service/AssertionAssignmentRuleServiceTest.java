package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.assertion.rule.AssertionAssignmentRuleFilter;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AssertionAssignmentRuleServiceTest {

  private static final Urn RULE_URN = UrnUtils.getUrn("urn:li:assertionAssignmentRule:test123");

  private SystemEntityClient mockEntityClient;
  private OperationContext opContext;
  private AssertionAssignmentRuleService service;
  private AssertionAssignmentRuleInfo ruleInfo;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    OpenApiClient mockOpenApiClient = mock(OpenApiClient.class);

    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    service =
        new AssertionAssignmentRuleService(mockEntityClient, mockOpenApiClient, new ObjectMapper());

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                CriterionUtils.buildCriterion(
                                    "platform",
                                    Condition.EQUAL,
                                    "urn:li:dataPlatform:snowflake")))));

    ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setName("test rule")
            .setEntityFilter(new AssertionAssignmentRuleFilter().setFilter(filter));
  }

  @Test
  public void testUpsertAutomation() throws Exception {
    service.upsertAssertionAssignmentRuleAutomation(opContext, RULE_URN, ruleInfo);

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(any(OperationContext.class), captor.capture(), anyBoolean());

    List<MetadataChangeProposal> proposals = captor.getValue();
    assertEquals(proposals.size(), 1);
    assertEquals(proposals.get(0).getAspectName(), TEST_INFO_ASPECT_NAME);
  }

  @Test
  public void testGetRuleInfo() throws Exception {
    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(ruleInfo.data())));
    response.setAspects(aspectMap);

    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME),
            eq(RULE_URN),
            anySet()))
        .thenReturn(response);

    AssertionAssignmentRuleInfo result = service.getRuleInfo(opContext, RULE_URN);

    assertNotNull(result);
    assertEquals(result.getName(), "test rule");
  }

  @Test
  public void testGetRuleInfoNotFound() throws Exception {
    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME),
            eq(RULE_URN),
            anySet()))
        .thenReturn(null);

    AssertionAssignmentRuleInfo result = service.getRuleInfo(opContext, RULE_URN);

    assertNull(result);
  }
}
