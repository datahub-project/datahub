package com.linkedin.metadata.dao.browse;

import com.linkedin.testing.EntityDocument;


public class TestBrowseConfig extends BaseBrowseConfig<EntityDocument> {

  @Override
  public Class<EntityDocument> getSearchDocument() {
    return EntityDocument.class;
  }
}
