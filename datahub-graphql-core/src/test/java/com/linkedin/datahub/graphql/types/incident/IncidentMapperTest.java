package com.linkedin.datahub.graphql.types.incident;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Incident;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import org.testng.annotations.Test;

public class IncidentMapperTest {

  @Test
  public void testMap() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:incident:1");
    Urn userUrn = Urn.createFromString("urn:li:corpuser:test");
    Urn assertionUrn = Urn.createFromString("urn:li:assertion:test");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedIncidentInfo = new EnvelopedAspect();
    IncidentInfo incidentInfo = new IncidentInfo();
    incidentInfo.setType(IncidentType.OPERATIONAL);
    incidentInfo.setCustomType("Custom Type");
    incidentInfo.setTitle("Test Incident", SetMode.IGNORE_NULL);
    incidentInfo.setDescription("This is a test incident", SetMode.IGNORE_NULL);
    incidentInfo.setPriority(1, SetMode.IGNORE_NULL);
    incidentInfo.setEntities(new UrnArray(Collections.singletonList(urn)));

    IncidentSource source = new IncidentSource();
    source.setType(IncidentSourceType.MANUAL);
    source.setSourceUrn(assertionUrn);
    incidentInfo.setSource(source);

    AuditStamp lastStatus = new AuditStamp();
    lastStatus.setTime(1000L);
    lastStatus.setActor(userUrn);
    incidentInfo.setCreated(lastStatus);

    IncidentStatus status = new IncidentStatus();
    status.setState(IncidentState.ACTIVE);
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
            new TagAssociationArray(
                Collections.singletonList(
                    new com.linkedin.common.TagAssociation()
                        .setTag(TagUrn.createFromString("urn:li:tag:test"))))));
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
        com.linkedin.datahub.graphql.generated.IncidentType.OPERATIONAL.toString());
    assertEquals(incident.getTitle(), "Test Incident");
    assertEquals(incident.getDescription(), "This is a test incident");
    assertEquals(incident.getPriority().intValue(), 1);
    assertEquals(
        incident.getSource().getType().toString(),
        com.linkedin.datahub.graphql.generated.IncidentSourceType.MANUAL.toString());
    assertEquals(incident.getSource().getSource().getUrn(), assertionUrn.toString());
    assertEquals(
        incident.getStatus().getState().toString(),
        com.linkedin.datahub.graphql.generated.IncidentState.ACTIVE.toString());
    assertEquals(incident.getStatus().getMessage(), "This incident is open.");
    assertEquals(incident.getStatus().getLastUpdated().getTime().longValue(), 1000L);
    assertEquals(incident.getStatus().getLastUpdated().getActor(), userUrn.toString());
    assertEquals(incident.getCreated().getTime().longValue(), 1000L);
    assertEquals(incident.getCreated().getActor(), userUrn.toString());

    assertEquals(incident.getTags().getTags().size(), 1);
    assertEquals(
        incident.getTags().getTags().get(0).getTag().getUrn().toString(), "urn:li:tag:test");
  }
}
