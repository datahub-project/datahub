package com.linkedin.metadata;

import com.datahub.test.BrowsePathEntry;
import com.datahub.test.BrowsePathEntryArray;
import com.datahub.test.KeyPartEnum;
import com.datahub.test.SearchFeatures;
import com.datahub.test.SimpleNestedRecord1;
import com.datahub.test.SimpleNestedRecord2;
import com.datahub.test.SimpleNestedRecord2Array;
import com.datahub.test.TestBrowsePaths;
import com.datahub.test.TestBrowsePathsV2;
import com.datahub.test.TestEntityAspect;
import com.datahub.test.TestEntityAspectArray;
import com.datahub.test.TestEntityInfo;
import com.datahub.test.TestEntityKey;
import com.datahub.test.TestEntitySnapshot;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.BooleanMap;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.FloatMap;
import com.linkedin.data.template.IntegerMap;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;

public class TestEntityUtil {
  private TestEntityUtil() {}

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
    testEntityInfo.setNestedRecordField(
        new SimpleNestedRecord1().setNestedIntegerField(1).setNestedForeignKey(urn));
    testEntityInfo.setNestedRecordArrayField(
        new SimpleNestedRecord2Array(
            ImmutableList.of(
                new SimpleNestedRecord2().setNestedArrayStringField("nestedArray1"),
                new SimpleNestedRecord2()
                    .setNestedArrayStringField("nestedArray2")
                    .setNestedArrayArrayField(
                        new StringArray(
                            ImmutableList.of("testNestedArray1", "testNestedArray2"))))));
    testEntityInfo.setCustomProperties(
        new StringMap(
            ImmutableMap.of(
                "key1",
                "value1",
                "key2",
                "value2",
                "shortValue",
                "123",
                "longValue",
                "0123456789")));
    testEntityInfo.setEsObjectField(
        new StringMap(
            ImmutableMap.of(
                "key1",
                "value1",
                "key2",
                "value2",
                "key3",
                "",
                "shortValue",
                "123",
                "longValue",
                "0123456789")));
    testEntityInfo.setDoubleField(100.456);
    testEntityInfo.setEsObjectFieldBoolean(
        new BooleanMap(ImmutableMap.of("key1", true, "key2", false)));
    testEntityInfo.setEsObjectFieldLong(new LongMap(ImmutableMap.of("key1", 1L, "key2", 2L)));
    testEntityInfo.setEsObjectFieldFloat(new FloatMap(ImmutableMap.of("key1", 1.0f, "key2", 2.0f)));
    testEntityInfo.setEsObjectFieldDouble(new DoubleMap(ImmutableMap.of("key1", 1.2, "key2", 2.4)));
    testEntityInfo.setEsObjectFieldInteger(
        new IntegerMap(ImmutableMap.of("key1", 123, "key2", 456)));
    return testEntityInfo;
  }

  public static TestEntitySnapshot getSnapshot() {
    TestEntitySnapshot snapshot = new TestEntitySnapshot();
    Urn urn = getTestEntityUrn();
    snapshot.setUrn(urn);

    TestBrowsePaths browsePaths =
        new TestBrowsePaths().setPaths(new StringArray(ImmutableList.of("/a/b/c", "d/e/f")));
    BrowsePathEntryArray browsePathV2Entries = new BrowsePathEntryArray();
    BrowsePathEntry entry1 = new BrowsePathEntry().setId("levelOne");
    BrowsePathEntry entry2 = new BrowsePathEntry().setId("levelTwo");
    browsePathV2Entries.add(entry1);
    browsePathV2Entries.add(entry2);
    TestBrowsePathsV2 browsePathsV2 = new TestBrowsePathsV2().setPath(browsePathV2Entries);
    SearchFeatures searchFeatures = new SearchFeatures().setFeature1(2).setFeature2(1);

    TestEntityAspectArray aspects =
        new TestEntityAspectArray(
            ImmutableList.of(
                TestEntityAspect.create(getTestEntityKey(urn)),
                TestEntityAspect.create(getTestEntityInfo(urn)),
                TestEntityAspect.create(browsePaths),
                TestEntityAspect.create(searchFeatures),
                TestEntityAspect.create(browsePathsV2)));
    snapshot.setAspects(aspects);
    return snapshot;
  }
}
