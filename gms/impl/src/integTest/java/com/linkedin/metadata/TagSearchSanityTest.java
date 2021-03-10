package com.linkedin.metadata;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.metadata.configs.TagSearchConfig;
import com.linkedin.metadata.search.TagDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;

import javax.annotation.Nonnull;

public class TagSearchSanityTest extends BaseSearchSanityTests<TagDocument> {
    @SearchIndexType(TagDocument.class)
    @SearchIndexSettings("/index/tags/settings.json")
    @SearchIndexMappings("/index/tags/mappings.json")
    public SearchIndex<TagDocument> _index;

    private static final TagUrn URN = new TagUrn("test-tag");
    private static final TagDocument DOCUMENT = new TagDocument()
            .setUrn(URN);

    protected TagSearchSanityTest() {
        super(URN, DOCUMENT, new TagSearchConfig());
    }

    @Nonnull
    @Override
    public SearchIndex<TagDocument> getIndex() {
        return _index;
    }
}
