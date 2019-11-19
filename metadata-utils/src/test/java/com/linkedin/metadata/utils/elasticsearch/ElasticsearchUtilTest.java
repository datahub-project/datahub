package com.linkedin.metadata.utils.elasticsearch;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class ElasticsearchUtilTest {

  @Test
  public void testPrefixFromDatasetName() {
    assertEquals(ElasticsearchUtil.getPrefixFromDatasetName("/jobs/act/takeout/follows/OTHER/empty_users_new"), "/jobs");
    assertEquals(ElasticsearchUtil.getPrefixFromDatasetName("tracking.pageviewevent"), "tracking");
    assertEquals(ElasticsearchUtil.getPrefixFromDatasetName("adAnalyticsEvent"), "adAnalyticsEvent");
  }

  @Test
  public void testConstructPath() {
    assertEquals(ElasticsearchUtil.constructPath("PROD", "hdfs", "/user/shyland/u_shyland.db/sh_cp_9"), "/prod/hdfs/user/shyland/u_shyland.db/sh_cp_9");
    assertEquals(ElasticsearchUtil.constructPath("ei", "seas-deployed", "/foo/bar.baz"), "/ei/seas-deployed/foo/bar.baz");
    assertEquals(ElasticsearchUtil.constructPath("corp", "hive", "u_scxu.prefilled_skills"), "/corp/hive/u_scxu/prefilled_skills");
    assertEquals(ElasticsearchUtil.constructPath("prod", "pinot", "foo"), "/prod/pinot/foo");
    assertEquals(ElasticsearchUtil.constructPath("ei", "testPlatform", "foo"), "/ei/testplatform/foo");
  }
}