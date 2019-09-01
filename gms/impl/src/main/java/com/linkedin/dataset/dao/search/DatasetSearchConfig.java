package com.linkedin.dataset.dao.search;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.search.DatasetDocument;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;


public class DatasetSearchConfig extends BaseSearchConfig<DatasetDocument> {
    @Override
    @Nonnull
    public Set<String> getFacetFields() {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList("origin", "platform")));
    }

    @Override
    @Nullable
    public Set<String> getLowCardinalityFields() {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList("origin", "platform")));
    }

    @Nonnull
    public Class getSearchDocument() {
        return DatasetDocument.class;
    }
}
