package com.linkedin.metadata;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.configs.DataProcessSearchConfig;
import com.linkedin.metadata.search.DataProcessDocument;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import javax.annotation.Nonnull;


public class DataProcessSearchSanityTest extends BaseSearchSanityTests<DataProcessDocument> {
  @SearchIndexType(DatasetDocument.class)
  @SearchIndexSettings("/index/data-process/settings.json")
  @SearchIndexMappings("/index/data-process/mappings.json")
  public SearchIndex<DataProcessDocument> _index;

  private static final DataProcessUrn URN = new DataProcessUrn("azkaban", "some.flow", FabricType.DEV);
  private static final DataProcessDocument DOCUMENT = new DataProcessDocument().setUrn(URN)
      .setBrowsePaths(new StringArray("some", "flow"))
      .setHasOwners(true)
      .setInputs(new DatasetUrnArray(new DatasetUrn(new DataPlatformUrn("hdfs"), "/foo/bar", FabricType.DEV)))
      .setName("some.flow")
      .setOrchestrator("azkaban");

  protected DataProcessSearchSanityTest() {
    super(URN, DOCUMENT, new DataProcessSearchConfig());
  }

  @Nonnull
  @Override
  public SearchIndex<DataProcessDocument> getIndex() {
    return _index;
  }
}
