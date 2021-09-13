package com.linkedin.metadata.dao;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.metadata.snapshot.TagSnapshot;

/**
 * An action request builder for tag entities.
 */
public class TagActionRequestBuilder extends BaseActionRequestBuilder<TagSnapshot, TagUrn> {

    private static final String BASE_URI_TEMPLATE = "tags";

    public TagActionRequestBuilder() {
        super(TagSnapshot.class, TagUrn.class, BASE_URI_TEMPLATE);
    }
}
