package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.DataFlowDocument;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class DataFlowSearchConfig extends BaseSearchConfig<DataFlowDocument> {
    @Override
    @Nonnull
    public Set<String> getFacetFields() {
        return Collections.emptySet();
    }

    @Override
    @Nonnull
    public Class<DataFlowDocument> getSearchDocument() {
        return DataFlowDocument.class;
    }

    @Override
    @Nonnull
    public String getDefaultAutocompleteField() {
        return "flowId";
    }

    @Override
    @Nonnull
    public String getSearchQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "dataFlowESSearchQueryTemplate.json");
    }

    @Override
    @Nonnull
    public String getAutocompleteQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "dataFlowESAutocompleteQueryTemplate.json");
    }
}
