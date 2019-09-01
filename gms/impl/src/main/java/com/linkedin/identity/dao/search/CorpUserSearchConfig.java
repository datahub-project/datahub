package com.linkedin.identity.dao.search;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;


public class CorpUserSearchConfig extends BaseSearchConfig<CorpUserInfoDocument> {
    @Override
    @Nonnull
    public Set<String> getFacetFields() {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList("title")));
    }

    @Nonnull
    public Class getSearchDocument() {
        return CorpUserInfoDocument.class;
    }
}