package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.eventgenerator.PropertiesDescriptionChangeEventGenerator.TrackedField;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class PropertiesDescriptionChangeEventGeneratorTest {

  private static final String TEST_URN = "urn:li:dataFlow:(airflow,test_dag,PROD)";

  private static PropertiesDescriptionChangeEventGenerator<DataFlowInfo> newGenerator() {
    return new PropertiesDescriptionChangeEventGenerator<>(
        DATA_FLOW_INFO_ASPECT_NAME,
        DataFlowInfo.class,
        List.of(TrackedField.description("description")),
        Map.of());
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

  private static final String NODE_URN = "urn:li:glossaryNode:node";

  private static PropertiesDescriptionChangeEventGenerator<GlossaryNodeInfo> nodeGenerator() {
    return new PropertiesDescriptionChangeEventGenerator<>(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        GlossaryNodeInfo.class,
        List.of(
            TrackedField.text("name", "Name"),
            TrackedField.description("definition"),
            TrackedField.parent("parentNode"),
            TrackedField.stringMap("customProperties", "Custom property")),
        Map.of());
  }

  private static GlossaryNodeInfo node(
      String name, String parent, Map<String, String> customProperties) {
    GlossaryNodeInfo info = new GlossaryNodeInfo().setDefinition("def");
    if (name != null) {
      info.setName(name);
    }
    if (parent != null) {
      info.setParentNode(new GlossaryNodeUrn(parent));
    }
    if (customProperties != null) {
      info.setCustomProperties(new StringMap(customProperties));
    }
    return info;
  }

  private static List<ChangeEvent> nodeDiff(GlossaryNodeInfo before, GlossaryNodeInfo after)
      throws Exception {
    return nodeGenerator()
        .getChangeEvents(
            Urn.createFromString(NODE_URN),
            GLOSSARY_NODE_ENTITY_NAME,
            GLOSSARY_NODE_INFO_ASPECT_NAME,
            new Aspect<>(before, new SystemMetadata()),
            new Aspect<>(after, new SystemMetadata()),
            testAuditStamp());
  }

  @Test
  public void testTextFieldModify() throws Exception {
    List<ChangeEvent> actual = nodeDiff(node("a", null, null), node("b", null, null));
    assertEquals(actual.size(), 1);
    ChangeEvent event = actual.get(0);
    assertEquals(event.getCategory(), ChangeCategory.DOCUMENTATION);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertEquals(event.getParameters().get("field"), "name");
    assertEquals(event.getParameters().get("value"), "b");
    assertEquals(event.getParameters().get("previousValue"), "a");
    assertEquals(
        event.getDescription(), "Name of '" + NODE_URN + "' has been changed from 'a' to 'b'.");
  }

  @Test
  public void testParentMoved() throws Exception {
    List<ChangeEvent> actual = nodeDiff(node("a", "n1", null), node("a", "n2", null));
    assertEquals(actual.size(), 1);
    ChangeEvent event = actual.get(0);
    assertEquals(event.getCategory(), ChangeCategory.PARENT);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertEquals(event.getParameters().get("oldParent"), "urn:li:glossaryNode:n1");
    assertEquals(event.getParameters().get("newParent"), "urn:li:glossaryNode:n2");
  }

  @Test
  public void testParentRemoved() throws Exception {
    List<ChangeEvent> actual = nodeDiff(node("a", "n1", null), node("a", null, null));
    assertEquals(actual.size(), 1);
    ChangeEvent event = actual.get(0);
    assertEquals(event.getCategory(), ChangeCategory.PARENT);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertEquals(event.getParameters().get("oldParent"), "urn:li:glossaryNode:n1");
    assertFalse(event.getParameters().containsKey("newParent"));
  }

  @Test
  public void testCustomPropertyAddedRemoved() throws Exception {
    List<ChangeEvent> actual =
        nodeDiff(node("a", null, Map.of("k1", "v1")), node("a", null, Map.of("k2", "v2")));
    assertEquals(actual.size(), 2);
    Map<ChangeOperation, ChangeEvent> byOperation =
        actual.stream().collect(Collectors.toMap(ChangeEvent::getOperation, e -> e));
    assertEquals(byOperation.get(ChangeOperation.REMOVE).getParameters().get("key"), "k1");
    assertEquals(byOperation.get(ChangeOperation.ADD).getParameters().get("key"), "k2");
    assertEquals(byOperation.get(ChangeOperation.ADD).getParameters().get("value"), "v2");
  }

  @Test
  public void testCustomPropertyValueChangeIsSingleModify() throws Exception {
    List<ChangeEvent> actual =
        nodeDiff(node("a", null, Map.of("k", "v1")), node("a", null, Map.of("k", "v2")));
    assertEquals(actual.size(), 1);
    assertEquals(actual.get(0).getOperation(), ChangeOperation.MODIFY);
    assertEquals(actual.get(0).getParameters().get("value"), "v2");
    assertEquals(actual.get(0).getParameters().get("previousValue"), "v1");
  }

  @Test
  public void testCustomPropertiesEmptyVsAbsentNoEvent() throws Exception {
    assertEquals(nodeDiff(node("a", null, Map.of()), node("a", null, null)).size(), 0);
    assertEquals(nodeDiff(node("a", null, null), node("a", null, Map.of())).size(), 0);
  }

  @Test
  public void testNullPreviousEmitsAddsOnlyForSetFields() throws Exception {
    List<ChangeEvent> actual =
        nodeGenerator()
            .getChangeEvents(
                Urn.createFromString(NODE_URN),
                GLOSSARY_NODE_ENTITY_NAME,
                GLOSSARY_NODE_INFO_ASPECT_NAME,
                new Aspect<>(null, new SystemMetadata()),
                new Aspect<>(node("a", null, null), new SystemMetadata()),
                testAuditStamp());
    assertEquals(actual.size(), 2);
    actual.forEach(e -> assertEquals(e.getOperation(), ChangeOperation.ADD));
  }

  private static EntityAspect storedNode(String json, long version) {
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(NODE_URN);
    aspect.setAspect(GLOSSARY_NODE_INFO_ASPECT_NAME);
    aspect.setVersion(version);
    aspect.setMetadata(json);
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:admin");
    return aspect;
  }

  @Test
  public void testSemanticDiffFiltersByCategory() {
    ChangeTransaction tx =
        nodeGenerator()
            .getSemanticDiff(
                storedNode(
                    "{\"definition\":\"def\",\"name\":\"a\",\"parentNode\":\"urn:li:glossaryNode:n1\"}",
                    0),
                storedNode(
                    "{\"definition\":\"def\",\"name\":\"b\",\"parentNode\":\"urn:li:glossaryNode:n2\"}",
                    1),
                ChangeCategory.PARENT,
                null,
                false);
    assertEquals(tx.getChangeEvents().size(), 1);
    assertEquals(tx.getChangeEvents().get(0).getCategory(), ChangeCategory.PARENT);
  }

  // The timeline diffs the first version against a metadata-less sentinel; creating a node under a
  // parent must not read as a move.
  @Test
  public void testSemanticDiffCreateUnderParentIsNotAMove() {
    EntityAspect sentinel = storedNode(null, -1);
    EntityAspect created =
        storedNode("{\"name\":\"a\",\"parentNode\":\"urn:li:glossaryNode:n1\"}", 0);
    assertEquals(
        nodeGenerator()
            .getSemanticDiff(sentinel, created, ChangeCategory.PARENT, null, false)
            .getChangeEvents()
            .size(),
        0);
    assertEquals(
        nodeGenerator()
            .getSemanticDiff(sentinel, created, ChangeCategory.DOCUMENTATION, null, false)
            .getChangeEvents()
            .size(),
        1);
  }
}
