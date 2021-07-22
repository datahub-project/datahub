package com.linkedin.metadata.extractor;

import com.datahub.test.TestEntityAspect;
import com.datahub.test.TestEntityAspectArray;
import com.datahub.test.TestEntityInfo;
import com.datahub.test.TestEntityKey;
import com.datahub.test.TestEntitySnapshot;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.TestEntityUtil;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class AspectExtractorTest {
  @Test
  public void testExtractor() {
    TestEntitySnapshot snapshot = new TestEntitySnapshot();
    // Empty snapshot should return empty map
    assertEquals(AspectExtractor.extractAspects(new TestEntitySnapshot()), ImmutableMap.of());

    Urn urn = TestEntityUtil.getTestEntityUrn();
    TestEntityKey testEntityKey = TestEntityUtil.getTestEntityKey(urn);
    TestEntityInfo testEntityInfo = TestEntityUtil.getTestEntityInfo(urn);
    snapshot.setAspects(
        new TestEntityAspectArray(TestEntityAspect.create(testEntityKey), TestEntityAspect.create(testEntityInfo)));
    Map<String, DataElement> result = AspectExtractor.extractAspects(snapshot);
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("testEntityKey"));
    assertTrue(result.containsKey("testEntityInfo"));

    Map<String, RecordTemplate> result2 = AspectExtractor.extractAspectRecords(snapshot);
    assertEquals(result2.size(), 2);
    assertEquals(result2.get("testEntityKey"), testEntityKey);
    assertEquals(result2.get("testEntityInfo"), testEntityInfo);
  }
}
