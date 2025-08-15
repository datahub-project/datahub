package com.linkedin.datahub.graphql.types.incident;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Incident;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import org.testng.annotations.Test;

public class IncidentMapperTest {

  @Test
  public void testMap() throws Exception {
    // This is your existing full test that sets up many fields.
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:incident:1");
    Urn userUrn = Urn.createFromString("urn:li:corpuser:test");
    Urn assertionUrn = Urn.createFromString("urn:li:assertion:test");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedIncidentInfo = new EnvelopedAspect();
    IncidentInfo incidentInfo = new IncidentInfo();

    AuditStamp lastStatus = new AuditStamp();
    lastStatus.setTime(1000L);
    lastStatus.setActor(userUrn);
    incidentInfo.setCreated(lastStatus);

    incidentInfo.setType(IncidentType.FIELD);
    incidentInfo.setCustomType("Custom Type");
    incidentInfo.setTitle("Test Incident", SetMode.IGNORE_NULL);
    incidentInfo.setDescription("This is a test incident", SetMode.IGNORE_NULL);
    // Set priority HIGH (1 -> HIGH)
    incidentInfo.setPriority(1, SetMode.IGNORE_NULL);
    incidentInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                    .setAssignedAt(lastStatus),
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpGroup:test2"))
                    .setAssignedAt(lastStatus))));
    // TODO: Support multiple entities per incident.
    incidentInfo.setEntities(new com.linkedin.common.UrnArray(Collections.singletonList(urn)));
    Long incidentStartedAt = 10L;
    incidentInfo.setStartedAt(incidentStartedAt);

    IncidentSource source = new IncidentSource();
    source.setType(IncidentSourceType.ASSERTION_FAILURE);
    source.setSourceUrn(assertionUrn);
    incidentInfo.setSource(source);

    IncidentStatus status = new IncidentStatus();
    status.setState(IncidentState.ACTIVE);
    status.setStage(IncidentStage.INVESTIGATION);
    status.setLastUpdated(lastStatus);
    status.setMessage("This incident is open.", SetMode.IGNORE_NULL);
    incidentInfo.setStatus(status);

    AuditStamp created = new AuditStamp();
    created.setTime(1000L);
    created.setActor(userUrn);
    incidentInfo.setCreated(created);

    envelopedIncidentInfo.setValue(new Aspect(incidentInfo.data()));

    EnvelopedAspect envelopedTagsAspect = new EnvelopedAspect();
    GlobalTags tags = new GlobalTags();
    tags.setTags(
        new TagAssociationArray(
            ImmutableList.of(
                new com.linkedin.common.TagAssociation()
                    .setTag(TagUrn.createFromString("urn:li:tag:test")))));
    envelopedTagsAspect.setValue(new Aspect(tags.data()));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.INCIDENT_INFO_ASPECT_NAME, envelopedIncidentInfo,
                Constants.GLOBAL_TAGS_ASPECT_NAME, envelopedTagsAspect)));

    Incident incident = IncidentMapper.map(null, entityResponse);

    assertNotNull(incident);
    assertEquals(incident.getUrn(), "urn:li:incident:1");
    assertEquals(incident.getType(), EntityType.INCIDENT);
    assertEquals(incident.getCustomType(), "Custom Type");
    assertEquals(
        incident.getIncidentType().toString(),
        com.linkedin.datahub.graphql.generated.IncidentType.FIELD.toString());
    assertEquals(incident.getTitle(), "Test Incident");
    assertEquals(incident.getDescription(), "This is a test incident");
    assertEquals(incident.getPriority(), IncidentPriority.HIGH);
    assertEquals(incident.getStartedAt(), incidentStartedAt);
    assertEquals(
        incident.getSource().getType().toString(),
        com.linkedin.datahub.graphql.generated.IncidentSourceType.ASSERTION_FAILURE.toString());
    assertEquals(incident.getSource().getSource().getUrn(), assertionUrn.toString());
    assertEquals(
        incident.getStatus().getState().toString(),
        com.linkedin.datahub.graphql.generated.IncidentState.ACTIVE.toString());
    assertEquals(
        incident.getStatus().getStage().toString(),
        com.linkedin.datahub.graphql.generated.IncidentStage.INVESTIGATION.toString());
    assertEquals(incident.getStatus().getMessage(), "This incident is open.");
    assertEquals(incident.getStatus().getLastUpdated().getTime().longValue(), 1000L);
    assertEquals(incident.getStatus().getLastUpdated().getActor(), userUrn.toString());
    assertEquals(incident.getCreated().getTime().longValue(), 1000L);
    assertEquals(incident.getCreated().getActor(), userUrn.toString());
    assertEquals(incident.getAssignees().size(), 2);
    assertEquals(((CorpUser) incident.getAssignees().get(0)).getUrn(), "urn:li:corpuser:test");
    assertEquals(((CorpGroup) incident.getAssignees().get(1)).getUrn(), "urn:li:corpGroup:test2");

    assertEquals(incident.getTags().getTags().size(), 1);
    assertEquals(
        incident.getTags().getTags().get(0).getTag().getUrn().toString(), "urn:li:tag:test");
  }

  // --- Additional tests for priority mapping ---

  @Test
  public void testMappingPriorityLow() throws Exception {
    // Priority 3 should map to LOW
    EntityResponse entityResponse = createBaseEntityResponse();
    setIncidentPriority(entityResponse, 3);
    Incident incident = IncidentMapper.map(null, entityResponse);
    assertEquals(
        incident.getPriority(), IncidentPriority.LOW, "Priority 3 should be mapped to LOW");
  }

  @Test
  public void testMappingPriorityMedium() throws Exception {
    // Priority 2 should map to MEDIUM
    EntityResponse entityResponse = createBaseEntityResponse();
    setIncidentPriority(entityResponse, 2);
    Incident incident = IncidentMapper.map(null, entityResponse);
    assertEquals(
        incident.getPriority(), IncidentPriority.MEDIUM, "Priority 2 should be mapped to MEDIUM");
  }

  @Test
  public void testMappingPriorityHigh() throws Exception {
    // Priority 1 should map to HIGH
    EntityResponse entityResponse = createBaseEntityResponse();
    setIncidentPriority(entityResponse, 1);
    Incident incident = IncidentMapper.map(null, entityResponse);
    assertEquals(
        incident.getPriority(), IncidentPriority.HIGH, "Priority 1 should be mapped to HIGH");
  }

  @Test
  public void testMappingPriorityCritical() throws Exception {
    // Priority 0 should map to CRITICAL
    EntityResponse entityResponse = createBaseEntityResponse();
    setIncidentPriority(entityResponse, 0);
    Incident incident = IncidentMapper.map(null, entityResponse);
    assertEquals(
        incident.getPriority(),
        IncidentPriority.CRITICAL,
        "Priority 0 should be mapped to CRITICAL");
  }

  @Test
  public void testMappingInvalidPriority() throws Exception {
    // An invalid priority (e.g., 5) should result in a null mapping.
    EntityResponse entityResponse = createBaseEntityResponse();
    setIncidentPriority(entityResponse, 5);
    Incident incident = IncidentMapper.map(null, entityResponse);
    assertNull(
        incident.getPriority(), "Invalid priority value should result in a null priority mapping");
  }

  // --- Additional tests for assignee mapping (CorpUser vs CorpGroup) ---

  @Test
  public void testMappingAssigneesMapping() throws Exception {
    // Create an incident with one corp user and one corp group
    EntityResponse entityResponse = createBaseEntityResponse();
    EnvelopedAspect incidentAspect =
        entityResponse.getAspects().get(Constants.INCIDENT_INFO_ASPECT_NAME);
    IncidentInfo incidentInfo = new IncidentInfo(incidentAspect.getValue().data());

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(2000L);
    // Set explicit assignees
    IncidentAssignee corpUserAssignee =
        new IncidentAssignee()
            .setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"))
            .setAssignedAt(auditStamp);
    IncidentAssignee corpGroupAssignee =
        new IncidentAssignee()
            .setActor(UrnUtils.getUrn("urn:li:corpGroup:testGroup"))
            .setAssignedAt(auditStamp);
    incidentInfo.setAssignees(
        new IncidentAssigneeArray(ImmutableList.of(corpUserAssignee, corpGroupAssignee)));
    incidentAspect.setValue(new Aspect(incidentInfo.data()));

    Incident incident = IncidentMapper.map(null, entityResponse);
    assertNotNull(incident.getAssignees());
    assertEquals(incident.getAssignees().size(), 2);
    assertTrue(
        incident.getAssignees().get(0) instanceof CorpUser,
        "Expected first assignee to be a CorpUser");
    assertTrue(
        incident.getAssignees().get(1) instanceof CorpGroup,
        "Expected second assignee to be a CorpGroup");
    assertEquals(((CorpUser) incident.getAssignees().get(0)).getUrn(), "urn:li:corpuser:testUser");
    assertEquals(
        ((CorpGroup) incident.getAssignees().get(1)).getUrn(), "urn:li:corpGroup:testGroup");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMappingInvalidAssignee() throws Exception {
    // Create an incident with an invalid assignee type.
    EntityResponse entityResponse = createBaseEntityResponse();
    EnvelopedAspect incidentAspect =
        entityResponse.getAspects().get(Constants.INCIDENT_INFO_ASPECT_NAME);
    IncidentInfo incidentInfo = new IncidentInfo(incidentAspect.getValue().data());

    // Use an actor URN that does not correspond to a corp user or corp group.
    IncidentAssignee invalidAssignee =
        new IncidentAssignee().setActor(UrnUtils.getUrn("urn:li:invalid:entity"));
    incidentInfo.setAssignees(new IncidentAssigneeArray(ImmutableList.of(invalidAssignee)));
    incidentAspect.setValue(new Aspect(incidentInfo.data()));

    // This call should throw IllegalArgumentException.
    IncidentMapper.map(null, entityResponse);
  }

  // --- Helper methods for creating a minimal base EntityResponse ---

  /** Creates a minimal EntityResponse with a basic IncidentInfo aspect. */
  private EntityResponse createBaseEntityResponse() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:incident:1");
    Urn userUrn = Urn.createFromString("urn:li:corpuser:test");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedIncidentInfo = new EnvelopedAspect();
    IncidentInfo incidentInfo = new IncidentInfo();

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(1000L);
    auditStamp.setActor(userUrn);
    incidentInfo.setCreated(auditStamp);
    incidentInfo.setType(IncidentType.FIELD);
    incidentInfo.setTitle("Base Incident", SetMode.IGNORE_NULL);
    incidentInfo.setDescription("Base incident description", SetMode.IGNORE_NULL);
    // Default priority; tests will override this.
    incidentInfo.setPriority(1, SetMode.IGNORE_NULL);
    // Provide a default assignee (corp user) so mapping works.
    incidentInfo.setAssignees(
        new IncidentAssigneeArray(
            ImmutableList.of(
                new IncidentAssignee()
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                    .setAssignedAt(auditStamp))));
    incidentInfo.setEntities(new com.linkedin.common.UrnArray(Collections.singletonList(urn)));

    envelopedIncidentInfo.setValue(new Aspect(incidentInfo.data()));

    EnvelopedAspectMap aspects =
        new EnvelopedAspectMap(
            ImmutableMap.of(Constants.INCIDENT_INFO_ASPECT_NAME, envelopedIncidentInfo));
    entityResponse.setAspects(aspects);
    return entityResponse;
  }

  /**
   * Updates the priority value on the IncidentInfo aspect contained in the given EntityResponse.
   */
  private void setIncidentPriority(EntityResponse entityResponse, int priority) throws Exception {
    EnvelopedAspect incidentAspect =
        entityResponse.getAspects().get(Constants.INCIDENT_INFO_ASPECT_NAME);
    IncidentInfo incidentInfo = new IncidentInfo(incidentAspect.getValue().data());
    incidentInfo.setPriority(priority, SetMode.IGNORE_NULL);
    incidentAspect.setValue(new Aspect(incidentInfo.data()));
  }
}
