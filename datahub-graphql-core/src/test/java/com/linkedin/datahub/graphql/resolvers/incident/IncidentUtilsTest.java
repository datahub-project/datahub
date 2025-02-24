package com.linkedin.datahub.graphql.resolvers.incident;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.datahub.graphql.generated.IncidentStatusInput;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class IncidentUtilsTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

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
}
