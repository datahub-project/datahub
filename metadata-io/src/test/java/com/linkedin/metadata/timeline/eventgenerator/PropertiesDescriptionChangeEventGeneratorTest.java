package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.List;
import org.testng.annotations.Test;

public class PropertiesDescriptionChangeEventGeneratorTest {

  private static final String TEST_URN = "urn:li:dataFlow:(airflow,test_dag,PROD)";

  private static PropertiesDescriptionChangeEventGenerator<DataFlowInfo> newGenerator() {
    return new PropertiesDescriptionChangeEventGenerator<>(
        DATA_FLOW_INFO_ASPECT_NAME, DataFlowInfo.class, "description");
  }

  private static AuditStamp testAuditStamp() throws Exception {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static DataFlowInfo info(String description) {
    DataFlowInfo info = new DataFlowInfo().setName("test_dag");
    if (description != null) {
      info.setDescription(description);
    }
    return info;
  }

  private static List<ChangeEvent> diff(String before, String after) throws Exception {
    return newGenerator()
        .getChangeEvents(
            Urn.createFromString(TEST_URN),
            DATA_FLOW_ENTITY_NAME,
            DATA_FLOW_INFO_ASPECT_NAME,
            new Aspect<>(info(before), new SystemMetadata()),
            new Aspect<>(info(after), new SystemMetadata()),
            testAuditStamp());
  }

  @Test
  public void testNoChange() throws Exception {
    assertEquals(diff("same", "same").size(), 0);
  }

  @Test
  public void testDescriptionAdded() throws Exception {
    List<ChangeEvent> actual = diff(null, "new docs");
    assertEquals(actual.size(), 1);
    assertEquals(actual.get(0).getOperation(), ChangeOperation.ADD);
    assertEquals(actual.get(0).getParameters().get("description"), "new docs");
  }

  @Test
  public void testDescriptionRemoved() throws Exception {
    List<ChangeEvent> actual = diff("old docs", null);
    assertEquals(actual.size(), 1);
    assertEquals(actual.get(0).getOperation(), ChangeOperation.REMOVE);
    assertEquals(actual.get(0).getParameters().get("description"), "old docs");
  }

  @Test
  public void testDescriptionModifiedIncludesPreviousDescription() throws Exception {
    List<ChangeEvent> actual = diff("old docs", "new docs");
    assertEquals(actual.size(), 1);
    assertEquals(actual.get(0).getOperation(), ChangeOperation.MODIFY);
    // The UI diff view renders an empty row without previousDescription on MODIFY.
    assertEquals(actual.get(0).getParameters().get("description"), "new docs");
    assertEquals(actual.get(0).getParameters().get("previousDescription"), "old docs");
  }

  private static EntityAspect storedAspect(String description, long version) {
    String json =
        description == null
            ? "{\"name\":\"test_dag\"}"
            : String.format("{\"name\":\"test_dag\",\"description\":\"%s\"}", description);
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(TEST_URN);
    aspect.setAspect(DATA_FLOW_INFO_ASPECT_NAME);
    aspect.setVersion(version);
    aspect.setMetadata(json);
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:admin");
    return aspect;
  }

  // getSemanticDiff is the path the Change History (timeline) API serves from; the getChangeEvents
  // cases above exercise the shared diff logic directly.
  @Test
  public void testSemanticDiffModifyIncludesPreviousDescription() {
    ChangeTransaction tx =
        newGenerator()
            .getSemanticDiff(
                storedAspect("old docs", 0),
                storedAspect("new docs", 1),
                ChangeCategory.DOCUMENTATION,
                null,
                false);
    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);
    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.DOCUMENTATION);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertEquals(event.getParameters().get("description"), "new docs");
    assertEquals(event.getParameters().get("previousDescription"), "old docs");
  }

  @Test
  public void testSemanticDiffIgnoresNonDocumentationCategory() {
    ChangeTransaction tx =
        newGenerator()
            .getSemanticDiff(
                storedAspect("old docs", 0),
                storedAspect("new docs", 1),
                ChangeCategory.TAG,
                null,
                false);
    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 0);
  }
}
