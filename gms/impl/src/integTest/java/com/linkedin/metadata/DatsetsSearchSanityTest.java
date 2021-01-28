package com.linkedin.metadata;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.configs.DatasetSearchConfig;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import javax.annotation.Nonnull;


public class DatsetsSearchSanityTest extends BaseSearchSanityTests<DatasetDocument> {
  @SearchIndexType(DatasetDocument.class)
  @SearchIndexSettings("/index/dataset/settings.json")
  @SearchIndexMappings("/index/dataset/mappings.json")
  public SearchIndex<DatasetDocument> _index;

  private static final DataPlatformUrn DATA_PLATFORM_URN = new DataPlatformUrn("hdfs");
  private static final DatasetUrn URN = new DatasetUrn(DATA_PLATFORM_URN, "/foo/bar/baz", FabricType.DEV);
  private static final DatasetDocument DOCUMENT = new DatasetDocument().setUrn(URN)
      .setBrowsePaths(new StringArray("foo", "bar", "baz"))
      .setDeprecated(false)
      .setDescription("test dataset")
      .setHasOwners(true)
      .setHasSchema(true)
      .setName("/foo/bar/baz")
      .setOrigin(FabricType.DEV)
      .setOwners(new StringArray("fbaggins"))
      .setPlatform("hdfs")
      .setRemoved(false);

  protected DatsetsSearchSanityTest() {
    super(URN, DOCUMENT, new DatasetSearchConfig());
  }

  @Nonnull
  @Override
  public SearchIndex<DatasetDocument> getIndex() {
    return _index;
  }
}
