package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.browse.BaseBrowseConfig;
import com.linkedin.metadata.search.DatasetDocument;

public class DatasetBrowseConfig extends BaseBrowseConfig<DatasetDocument> {
  public Class getSearchDocument() {
    return DatasetDocument.class;
  }
}
