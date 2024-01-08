package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;
import static org.testng.AssertJUnit.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import java.util.List;
import java.util.Map;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class IncidentInfoChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  static final Urn urn = UrnUtils.getUrn("urn:li:incident:16ff200a-0ac5-4a7d-bbab-d4bdb4f831f9");
  static final String entity = Constants.INCIDENT_ENTITY_NAME;
  static final String aspect = Constants.INCIDENT_INFO_ASPECT_NAME;

  @Test
  public void testCreate() {
    final IncidentInfoChangeEventGenerator test = new IncidentInfoChangeEventGenerator();
    final Aspect<IncidentInfo> oldIncident =
        new Aspect<>(null, SystemMetadataUtils.createDefaultSystemMetadata());
    final IncidentInfo info = new IncidentInfo();
    info.setStatus(new IncidentStatus().setState(IncidentState.ACTIVE));
    info.setEntities(
        new UrnArray(
            ImmutableList.of(
                UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"),
                UrnUtils.getUrn("urn:li:dataset:abc"))));
    info.setCreated(AuditStampUtils.createDefaultAuditStamp());
    final Aspect<IncidentInfo> newIncident =
        new Aspect<>(info, SystemMetadataUtils.createDefaultSystemMetadata());
    final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

    List<ChangeEvent> actual =
        test.getChangeEvents(urn, entity, aspect, oldIncident, newIncident, auditStamp);
    assertEquals(1, actual.size());

    ChangeEvent changeEvent = actual.get(0);
    assertEquals(ChangeCategory.INCIDENT, changeEvent.getCategory());
    assertEquals(ChangeOperation.ACTIVE, changeEvent.getOperation());

    Map<String, Object> expectedParameters = ImmutableMap.of(ENTITY_REF, info.getEntities());
    assertEquals(expectedParameters, changeEvent.getParameters());
  }

  @Test
  public void testResolved() throws Exception {
    final IncidentInfoChangeEventGenerator test = new IncidentInfoChangeEventGenerator();
    final IncidentInfo oldInfo = new IncidentInfo();
    oldInfo.setStatus(new IncidentStatus().setState(IncidentState.ACTIVE));
    oldInfo.setEntities(
        new UrnArray(
            ImmutableList.of(
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"))));
    oldInfo.setCreated(AuditStampUtils.createDefaultAuditStamp());
    final Aspect<IncidentInfo> oldIncident =
        new Aspect<>(oldInfo, SystemMetadataUtils.createDefaultSystemMetadata());
    IncidentInfo newInfo = oldInfo.clone();
    newInfo.setStatus(new IncidentStatus().setState(IncidentState.RESOLVED));
    newInfo.setCreated(AuditStampUtils.createDefaultAuditStamp());
    final Aspect<IncidentInfo> newIncident =
        new Aspect<>(newInfo, SystemMetadataUtils.createDefaultSystemMetadata());
    final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

    List<ChangeEvent> actual =
        test.getChangeEvents(urn, entity, aspect, oldIncident, newIncident, auditStamp);
    assertEquals(1, actual.size());

    ChangeEvent changeEvent = actual.get(0);
    assertEquals(ChangeCategory.INCIDENT, changeEvent.getCategory());
    assertEquals(ChangeOperation.RESOLVED, changeEvent.getOperation());

    Map<String, Object> expectedParameters = ImmutableMap.of(ENTITY_REF, newInfo.getEntities());
    assertEquals(expectedParameters, changeEvent.getParameters());
  }

  @Test
  public void testUpdatingNonStatus() throws Exception {
    final IncidentInfoChangeEventGenerator test = new IncidentInfoChangeEventGenerator();
    final IncidentInfo oldInfo = new IncidentInfo();
    oldInfo.setStatus(new IncidentStatus().setState(IncidentState.ACTIVE));
    oldInfo.setEntities(
        new UrnArray(
            ImmutableList.of(
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"))));
    oldInfo.setCreated(AuditStampUtils.createDefaultAuditStamp());
    final Aspect<IncidentInfo> oldIncident =
        new Aspect<>(oldInfo, SystemMetadataUtils.createDefaultSystemMetadata());
    IncidentInfo newInfo = oldInfo.clone();
    newInfo.setTitle("Some title");
    newInfo.setCreated(AuditStampUtils.createDefaultAuditStamp());
    final Aspect<IncidentInfo> newIncident =
        new Aspect<>(newInfo, SystemMetadataUtils.createDefaultSystemMetadata());
    final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

    List<ChangeEvent> actual =
        test.getChangeEvents(urn, entity, aspect, oldIncident, newIncident, auditStamp);
    assertEquals(0, actual.size());
  }
}
