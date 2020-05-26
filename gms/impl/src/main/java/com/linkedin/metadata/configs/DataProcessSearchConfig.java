package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.DataProcessDocument;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class DataProcessSearchConfig extends BaseSearchConfig<DataProcessDocument> {
    @Override
    @Nonnull
    public Set<String> getFacetFields() {
        return Collections.emptySet();
    }

    @Override
    @Nonnull
    public Class<DataProcessDocument> getSearchDocument() {
        return DataProcessDocument.class;
    }

    @Override
    @Nonnull
    public String getDefaultAutocompleteField() {
        return "name";
    }

    @Override
    @Nonnull
    public String getSearchQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "dataProcessESSearchQueryTemplate.json");
    }

    @Override
    @Nonnull
    public String getAutocompleteQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "dataProcessESAutocompleteQueryTemplate.json");
    }
}
