package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.browse.BaseBrowseConfig;
import com.linkedin.metadata.search.GlossaryTermInfoDocument;

public class GlossaryTermBrowseConfig extends BaseBrowseConfig<GlossaryTermInfoDocument> {
  public Class getSearchDocument() {
    return GlossaryTermInfoDocument.class;
  }
}
