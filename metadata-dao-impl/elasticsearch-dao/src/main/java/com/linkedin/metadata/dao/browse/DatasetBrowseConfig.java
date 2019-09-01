package com.linkedin.metadata.dao.browse;

import com.linkedin.metadata.search.DatasetDocument;

public class DatasetBrowseConfig extends BaseBrowseConfig<DatasetDocument> {
    public Class getSearchDocument() {
        return DatasetDocument.class;
    }
}
