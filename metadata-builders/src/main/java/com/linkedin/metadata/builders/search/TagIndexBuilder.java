package com.linkedin.metadata.builders.search;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.search.TagDocument;
import com.linkedin.metadata.snapshot.TagSnapshot;
import com.linkedin.tag.TagProperties;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class TagIndexBuilder extends BaseIndexBuilder<TagDocument> {
    public TagIndexBuilder() {
        super(Collections.singletonList(TagSnapshot.class), TagDocument.class);
    }

    @Nonnull
    private static TagDocument setUrnDerivedFields(@Nonnull TagUrn urn) {
        return new TagDocument()
                .setUrn(urn)
                .setName(urn.getName());
    }

    @Nonnull
    private TagDocument getDocumentToUpdateFromAspect(@Nonnull TagUrn urn,
                                                        @Nonnull TagProperties tagProperties) {
        return setUrnDerivedFields(urn)
                .setName(tagProperties.getName());
    }

    @Nonnull
    private List<TagDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull TagSnapshot snapshot) {
        TagUrn urn = snapshot.getUrn();
        return snapshot.getAspects().stream().map(aspect -> {
            if (aspect.isTagProperties()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getTagProperties());
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public List<TagDocument> getDocumentsToUpdate(@Nonnull RecordTemplate snapshot) {
        if (snapshot instanceof TagSnapshot) {
            return getDocumentsToUpdateFromSnapshotType((TagSnapshot) snapshot);
        }
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Class<TagDocument> getDocumentType() {
        return TagDocument.class;
    }
}
