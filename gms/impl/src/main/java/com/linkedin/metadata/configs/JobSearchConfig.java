package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.search.JobDocument;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class JobSearchConfig extends BaseSearchConfig<JobDocument> {
    @Override
    @Nonnull
    public Set<String> getFacetFields() {
        return Collections.emptySet();
    }

    @Override
    @Nonnull
    public Class<JobDocument> getSearchDocument() {
        return JobDocument.class;
    }

    @Override
    @Nonnull
    public String getDefaultAutocompleteField() {
        return "name";
    }

    @Override
    @Nonnull
    public String getSearchQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "jobESSearchQueryTemplate.json");
    }

    @Override
    @Nonnull
    public String getAutocompleteQueryTemplate() {
        return SearchUtils.readResourceFile(getClass(), "jobESAutocompleteQueryTemplate.json");
    }
}
