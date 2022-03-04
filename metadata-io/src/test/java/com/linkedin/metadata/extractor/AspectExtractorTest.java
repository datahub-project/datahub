package com.linkedin.metadata.extractor;

import com.datahub.test.TestEntityAspect;
import com.datahub.test.TestEntityAspectArray;
import com.datahub.test.TestEntityInfo;
import com.datahub.test.TestEntityKey;
import com.datahub.test.TestEntitySnapshot;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.TestEntityUtil;
import com.linkedin.metadata.models.extractor.AspectExtractor;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;


public class AspectExtractorTest {
  @Test
  public void testExtractor() {
    TestEntitySnapshot snapshot = new TestEntitySnapshot();
    Urn urn = TestEntityUtil.getTestEntityUrn();
    TestEntityKey testEntityKey = TestEntityUtil.getTestEntityKey(urn);
    TestEntityInfo testEntityInfo = TestEntityUtil.getTestEntityInfo(urn);
    snapshot.setAspects(
        new TestEntityAspectArray(TestEntityAspect.create(testEntityKey), TestEntityAspect.create(testEntityInfo)));
    Map<String, RecordTemplate> result = AspectExtractor.extractAspectRecords(snapshot);
    assertEquals(result.size(), 2);
    assertEquals(result.get("testEntityKey"), testEntityKey);
    assertEquals(result.get("testEntityInfo"), testEntityInfo);
  }
}
