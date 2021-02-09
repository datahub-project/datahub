package com.linkedin.metadata;

import com.linkedin.chart.ChartQueryType;
import com.linkedin.chart.ChartType;
import com.linkedin.common.AccessLevel;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.configs.ChartSearchConfig;
import com.linkedin.metadata.search.ChartDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import javax.annotation.Nonnull;


public class ChartSearchSanityTest extends BaseSearchSanityTests<ChartDocument> {
  @SearchIndexType(ChartDocument.class)
  @SearchIndexSettings("/index/chart/settings.json")
  @SearchIndexMappings("/index/chart/mappings.json")
  public SearchIndex<ChartDocument> _index;

  private static final ChartUrn URN = new ChartUrn("testTool", "testId");
  private static final ChartDocument DOCUMENT = new ChartDocument().setAccess(AccessLevel.PUBLIC)
      .setDescription("a test chart")
      .setOwners(new StringArray("fbaggings"))
      .setQueryType(ChartQueryType.SQL)
      .setTitle("test chart")
      .setTool("testTool")
      .setType(ChartType.TEXT)
      .setUrn(URN);

  protected ChartSearchSanityTest() {
    super(URN, DOCUMENT, new ChartSearchConfig());
  }

  @Nonnull
  @Override
  public SearchIndex<ChartDocument> getIndex() {
    return _index;
  }
}
