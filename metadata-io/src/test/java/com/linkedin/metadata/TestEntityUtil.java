package com.linkedin.metadata;

import com.datahub.test.BrowsePaths;
import com.datahub.test.KeyPartEnum;
import com.datahub.test.SearchFeatures;
import com.datahub.test.SimpleNestedRecord1;
import com.datahub.test.SimpleNestedRecord2;
import com.datahub.test.SimpleNestedRecord2Array;
import com.datahub.test.TestEntityAspect;
import com.datahub.test.TestEntityAspectArray;
import com.datahub.test.TestEntityInfo;
import com.datahub.test.TestEntityKey;
import com.datahub.test.TestEntitySnapshot;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;


public class TestEntityUtil {
  private TestEntityUtil() {
  }

  public static Urn getTestEntityUrn() {
    return new TestEntityUrn("key", "urn", "VALUE_1");
  }

  public static TestEntityKey getTestEntityKey(Urn urn) {
    return new TestEntityKey().setKeyPart1("key").setKeyPart2(urn).setKeyPart3(KeyPartEnum.VALUE_1);
  }

  public static TestEntityInfo getTestEntityInfo(Urn urn) {
    TestEntityInfo testEntityInfo = new TestEntityInfo();
    testEntityInfo.setTextField("test");
    testEntityInfo.setTextArrayField(new StringArray(ImmutableList.of("testArray1", "testArray2")));
    testEntityInfo.setNestedRecordField(new SimpleNestedRecord1().setNestedIntegerField(1).setNestedForeignKey(urn));
    testEntityInfo.setNestedRecordArrayField(new SimpleNestedRecord2Array(
        ImmutableList.of(new SimpleNestedRecord2().setNestedArrayStringField("nestedArray1"),
            new SimpleNestedRecord2().setNestedArrayStringField("nestedArray2")
                .setNestedArrayArrayField(new StringArray(ImmutableList.of("testNestedArray1", "testNestedArray2"))))));
    testEntityInfo.setCustomProperties(new StringMap(ImmutableMap.of("key1", "value1", "key2", "value2")));
    testEntityInfo.setEsObjectField(new StringMap(ImmutableMap.of("key1", "value1", "key2", "value2")));
    return testEntityInfo;
  }

  public static TestEntitySnapshot getSnapshot() {
    TestEntitySnapshot snapshot = new TestEntitySnapshot();
    Urn urn = getTestEntityUrn();
    snapshot.setUrn(urn);

    BrowsePaths browsePaths = new BrowsePaths().setPaths(new StringArray(ImmutableList.of("/a/b/c", "d/e/f")));
    SearchFeatures searchFeatures = new SearchFeatures().setFeature1(2).setFeature2(1);

    TestEntityAspectArray aspects = new TestEntityAspectArray(
        ImmutableList.of(TestEntityAspect.create(getTestEntityKey(urn)),
            TestEntityAspect.create(getTestEntityInfo(urn)), TestEntityAspect.create(browsePaths),
            TestEntityAspect.create(searchFeatures)));
    snapshot.setAspects(aspects);
    return snapshot;
  }
}
