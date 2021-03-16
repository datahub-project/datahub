package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.metadata.dao.BaseActionRequestBuilder;
import com.linkedin.metadata.snapshot.TagSnapshot;

/**
 * An action request builder for tag entities.
 */
public class TagActionRequestBuilders extends BaseActionRequestBuilder<TagSnapshot, TagUrn> {

    private static final String BASE_URI_TEMPLATE = "tags";

    public TagActionRequestBuilders() {
        super(TagSnapshot.class, TagUrn.class, BASE_URI_TEMPLATE);
    }
}
