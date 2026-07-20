package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class GlobalTagsChangeEventGeneratorTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_TAG_URN = "urn:li:tag:TestTag";
  private static final String TEST_ACTION_URN = "urn:li:dataHubAction:test";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static GlobalTags globalTagsWith(TagAssociation tagAssociation) {
    return new GlobalTags().setTags(new TagAssociationArray(List.of(tagAssociation)));
  }

  @Test
  public void testAddedTagWithAttributionEmitsSourceDetails() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    StringMap sourceDetail =
        new StringMap(
            ImmutableMap.of(
                "propagated",
                "true",
                "propagation_direction",
                "downstream",
                "propagation_relationship",
                "lineage",
                "propagation_depth",
                "1"));
    TagAssociation tagAssociation =
        new TagAssociation()
            .setTag(TagUrn.createFromString(TEST_TAG_URN))
            .setAttribution(
                new MetadataAttribution()
                    .setTime(auditStamp.getTime())
                    .setActor(auditStamp.getActor())
                    .setSource(Urn.createFromString(TEST_ACTION_URN))
                    .setSourceDetail(sourceDetail));

    List<ChangeEvent> events =
        GlobalTagsChangeEventGenerator.computeDiffs(
            null, globalTagsWith(tagAssociation), TEST_ENTITY_URN, auditStamp);

    assertEquals(events.size(), 1);
    Map<String, Object> parameters = events.get(0).getParameters();
    assertEquals(parameters.get("tagUrn"), TEST_TAG_URN);
    assertEquals(parameters.get("context"), "{}");

    // sourceDetails is a JSON-stringified copy of the attribution's sourceDetail map.
    Map<String, String> parsedSourceDetails =
        OBJECT_MAPPER.readValue(
            (String) parameters.get("sourceDetails"),
            OBJECT_MAPPER.getTypeFactory().constructMapType(Map.class, String.class, String.class));
    assertEquals(parsedSourceDetails, sourceDetail);
  }

  @Test
  public void testAddedTagWithoutAttributionEmitsEmptySourceDetails() throws Exception {
    AuditStamp auditStamp = getTestAuditStamp();

    TagAssociation tagAssociation =
        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_URN));

    List<ChangeEvent> events =
        GlobalTagsChangeEventGenerator.computeDiffs(
            null, globalTagsWith(tagAssociation), TEST_ENTITY_URN, auditStamp);

    assertEquals(events.size(), 1);
    Map<String, Object> parameters = events.get(0).getParameters();
    assertEquals(parameters.get("tagUrn"), TEST_TAG_URN);
    assertEquals(parameters.get("context"), "{}");
    assertEquals(parameters.get("sourceDetails"), "{}");
  }
}
