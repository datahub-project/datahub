package com.linkedin.metadata.builders.search;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.search.DataProcessDocument;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

@Slf4j
public class DataProcessIndexBuilder extends BaseIndexBuilder<DataProcessDocument> {

    public DataProcessIndexBuilder() {
        super(Collections.singletonList(DataProcessSnapshot.class), DataProcessDocument.class);
    }

    @Nullable
    @Override
    public List<DataProcessDocument> getDocumentsToUpdate(@Nonnull RecordTemplate snapshot) {
        throw new UnsupportedOperationException(
                String.format("%s doesn't support this feature yet,",
                        this.getClass().getName())
        );
    }

    @Nonnull
    @Override
    public Class<DataProcessDocument> getDocumentType() {
        return DataProcessDocument.class;
    }
}
