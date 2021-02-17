package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.MLModelDocument;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class MLModelSearchConfig extends BaseSearchConfig<MLModelDocument> {
    @Override
    @Nonnull
    public Set<String> getFacetFields() {
        return Collections.emptySet();
    }

    @Override
    @Nonnull
    public Class<MLModelDocument> getSearchDocument() {
        return MLModelDocument.class;
    }

    @Override
    @Nonnull
    public String getDefaultAutocompleteField() {
        return "name";
    }

    @Override
    @Nonnull
    public String getSearchQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "mlModelESSearchQueryTemplate.json");
    }

    @Override
    @Nonnull
    public String getAutocompleteQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "mlModelESAutocompleteQueryTemplate.json");
    }
}
