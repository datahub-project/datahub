package com.linkedin.metadata;

import com.linkedin.common.AccessLevel;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.configs.DashboardSearchConfig;
import com.linkedin.metadata.search.DashboardDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import javax.annotation.Nonnull;


public class DashboardSearchSanityTest extends BaseSearchSanityTests<DashboardDocument> {
  @SearchIndexType(DashboardDocument.class)
  @SearchIndexSettings("/index/dashboard/settings.json")
  @SearchIndexMappings("/index/dashboard/mappings.json")
  public SearchIndex<DashboardDocument> _index;

  private static final DashboardUrn URN = new DashboardUrn("testTool", "testId");
  private static final DashboardDocument DOCUMENT = new DashboardDocument().setAccess(AccessLevel.PUBLIC)
      .setBrowsePaths(new StringArray("foo", "bar"))
      .setDescription("a test dashboard")
      .setOwners(new StringArray("fbaggings"))
      .setTitle("the test dashboard")
      .setTool("the tool")
      .setUrn(URN);

  protected DashboardSearchSanityTest() {
    super(URN, DOCUMENT, new DashboardSearchConfig());
  }

  @Nonnull
  @Override
  public SearchIndex<DashboardDocument> getIndex() {
    return _index;
  }
}
