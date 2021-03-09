package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.DataJobDocument;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class DataJobSearchConfig extends BaseSearchConfig<DataJobDocument> {
    @Override
    @Nonnull
    public Set<String> getFacetFields() {
        return Collections.emptySet();
    }

    @Override
    @Nonnull
    public Class<DataJobDocument> getSearchDocument() {
        return DataJobDocument.class;
    }

    @Override
    @Nonnull
    public String getDefaultAutocompleteField() {
        return "jobId";
    }

    @Override
    @Nonnull
    public String getSearchQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "dataJobESSearchQueryTemplate.json");
    }

    @Override
    @Nonnull
    public String getAutocompleteQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "dataJobESAutocompleteQueryTemplate.json");
    }
}
