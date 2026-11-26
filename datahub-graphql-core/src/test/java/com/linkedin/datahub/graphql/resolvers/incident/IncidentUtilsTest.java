package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContextForResource;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.datahub.graphql.generated.IncidentStatusInput;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class IncidentUtilsTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)");
  private static final Urn TEST_OTHER_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,OtherTable,PROD)");
  private static final Urn TEST_CHART_URN = UrnUtils.getUrn("urn:li:chart:(looker,baz)");
  private static final Urn TEST_SCHEMA_FIELD_URN =
      SchemaFieldUtils.generateSchemaFieldUrn(TEST_DATASET_URN, "user_id");
  private static final String EDIT_INCIDENTS =
      PoliciesConfig.EDIT_ENTITY_INCIDENTS_PRIVILEGE.getType();

  @Test
  public void testMapIncidentPriorityWithNull() {
    assertNull(IncidentUtils.mapIncidentPriority(null));
  }

  @Test
  public void testMapIncidentPriorityWithLow() {
    assertEquals(IncidentUtils.mapIncidentPriority(IncidentPriority.LOW), Integer.valueOf(3));
  }

  @Test
  public void testMapIncidentPriorityWithMedium() {
    assertEquals(IncidentUtils.mapIncidentPriority(IncidentPriority.MEDIUM), Integer.valueOf(2));
  }

  @Test
  public void testMapIncidentPriorityWithHigh() {
    assertEquals(IncidentUtils.mapIncidentPriority(IncidentPriority.HIGH), Integer.valueOf(1));
  }

  @Test
  public void testMapIncidentPriorityWithCritical() {
    assertEquals(IncidentUtils.mapIncidentPriority(IncidentPriority.CRITICAL), Integer.valueOf(0));
  }

  @Test
  public void testMapIncidentAssigneesWithNullAssignees() {
    AuditStamp stamp = new AuditStamp();
    stamp.setActor(TEST_USER_URN);
    stamp.setTime(System.currentTimeMillis());
    assertNull(IncidentUtils.mapIncidentAssignees(null, stamp));
  }

  @Test
  public void testMapIncidentAssigneesWithEmptyAssignees() {
    AuditStamp stamp = new AuditStamp();
    stamp.setActor(TEST_USER_URN);
    stamp.setTime(System.currentTimeMillis());
    IncidentAssigneeArray result =
        IncidentUtils.mapIncidentAssignees(Collections.emptyList(), stamp);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testMapIncidentAssigneesWithValidAssignees() {
    AuditStamp stamp = new AuditStamp();
    stamp.setActor(TEST_USER_URN);
    stamp.setTime(System.currentTimeMillis());
    List<String> assignees = Arrays.asList("urn:li:corpuser:1", "urn:li:corpuser:2");
    IncidentAssigneeArray result = IncidentUtils.mapIncidentAssignees(assignees, stamp);
    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getActor().toString(), "urn:li:corpuser:1");
    assertEquals(result.get(1).getActor().toString(), "urn:li:corpuser:2");
  }

  @Test
  public void testMapIncidentStatusWithNullInput() {
    AuditStamp stamp = new AuditStamp();
    stamp.setActor(TEST_USER_URN);
    stamp.setTime(System.currentTimeMillis());
    IncidentStatus status = IncidentUtils.mapIncidentStatus(null, stamp);
    assertNotNull(status);
    assertEquals(status.getState(), IncidentState.ACTIVE);
    assertEquals(status.getLastUpdated(), stamp);
  }

  @Test
  public void testMapIncidentStatusWithValidInput() {
    IncidentStatusInput input = new IncidentStatusInput();
    input.setState(com.linkedin.datahub.graphql.generated.IncidentState.RESOLVED);
    input.setStage(com.linkedin.datahub.graphql.generated.IncidentStage.INVESTIGATION);
    input.setMessage("Issue resolved");

    AuditStamp stamp = new AuditStamp();
    stamp.setActor(TEST_USER_URN);
    stamp.setTime(System.currentTimeMillis());
    IncidentStatus status = IncidentUtils.mapIncidentStatus(input, stamp);

    assertEquals(status.getState(), IncidentState.RESOLVED);
    assertEquals(status.getStage(), IncidentStage.INVESTIGATION);
    assertEquals(status.getMessage(), "Issue resolved");
  }

  @Test
  public void testGetIncidentAuthorizationUrnRemapsSchemaFieldToParent() {
    assertEquals(
        IncidentUtils.getIncidentAuthorizationUrn(TEST_SCHEMA_FIELD_URN), TEST_DATASET_URN);
  }

  @Test
  public void testGetIncidentAuthorizationUrnLeavesOtherEntitiesAlone() {
    assertEquals(IncidentUtils.getIncidentAuthorizationUrn(TEST_DATASET_URN), TEST_DATASET_URN);
    assertEquals(IncidentUtils.getIncidentAuthorizationUrn(TEST_CHART_URN), TEST_CHART_URN);
  }

  @Test
  public void testGetIncidentAuthorizationUrnFallsBackWhenFieldUrnDoesNotParse() {
    // A schemaField URN missing its field path cannot yield a parent, so the check stays on the
    // URN it was given rather than failing open.
    Urn malformed = UrnUtils.getUrn("urn:li:schemaField:notAFieldUrn");
    assertEquals(IncidentUtils.getIncidentAuthorizationUrn(malformed), malformed);
  }

  @Test
  public void testEditIncidentOnSchemaFieldAllowedByParentDatasetPolicy() {
    QueryContext context =
        getMockAllowContextForResource(TEST_USER_URN.toString(), EDIT_INCIDENTS, TEST_DATASET_URN);
    assertTrue(IncidentUtils.isAuthorizedToEditIncidentForResource(TEST_SCHEMA_FIELD_URN, context));
  }

  @Test
  public void testEditIncidentOnSchemaFieldDeniedWhenParentPolicyDoesNotMatch() {
    QueryContext context =
        getMockAllowContextForResource(
            TEST_USER_URN.toString(), EDIT_INCIDENTS, TEST_OTHER_DATASET_URN);
    assertFalse(
        IncidentUtils.isAuthorizedToEditIncidentForResource(TEST_SCHEMA_FIELD_URN, context));
  }

  @Test
  public void testEditIncidentOnSchemaFieldUsesNonDatasetParent() {
    Urn chartFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(TEST_CHART_URN, "user_id");
    QueryContext context =
        getMockAllowContextForResource(TEST_USER_URN.toString(), EDIT_INCIDENTS, TEST_CHART_URN);
    assertTrue(IncidentUtils.isAuthorizedToEditIncidentForResource(chartFieldUrn, context));
  }

  @Test
  public void testEditIncidentOnOtherEntitiesIsUnchanged() {
    QueryContext context =
        getMockAllowContextForResource(TEST_USER_URN.toString(), EDIT_INCIDENTS, TEST_DATASET_URN);
    assertTrue(IncidentUtils.isAuthorizedToEditIncidentForResource(TEST_DATASET_URN, context));
    // The remap is scoped to schemaField: a dataset policy still does not reach other entities.
    assertFalse(IncidentUtils.isAuthorizedToEditIncidentForResource(TEST_CHART_URN, context));
  }
}
