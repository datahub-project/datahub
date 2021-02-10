package com.linkedin.metadata;

import java.util.Collections;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.configs.MLModelSearchConfig;
import com.linkedin.metadata.search.MLModelDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import javax.annotation.Nonnull;


public class MLModelSearchSanityTest extends BaseSearchSanityTests<MLModelDocument> {
    @SearchIndexType(MLModelDocument.class)
    @SearchIndexSettings("/index/ml-model/settings.json")
    @SearchIndexMappings("/index/ml-model/mappings.json")
    public SearchIndex<MLModelDocument> _index;

    private static final DataPlatformUrn DATA_PLATFORM_URN = new DataPlatformUrn("hdfs");
    private static final MLModelUrn URN = new MLModelUrn(DATA_PLATFORM_URN, "/foo/bar/baz", FabricType.DEV);
    private static final DatasetUrn DATASET_URN = new DatasetUrn(DATA_PLATFORM_URN, "/foo/bar/baz", FabricType.DEV);
    private static final DatasetUrnArray DATASET_URN_ARRAY = new DatasetUrnArray(Collections.singletonList(DATASET_URN));
    private static final MLModelDocument DOCUMENT = new MLModelDocument().setUrn(URN)
        .setDescription("test model")
        .setHasOwners(true)
        .setName("/foo/bar/baz")
        .setOrigin(FabricType.DEV)
        .setOwners(new StringArray("fbaggins"))
        .setTrainingDatasets(DATASET_URN_ARRAY)
        .setEvaluationDatasets(DATASET_URN_ARRAY)
        .setPlatform("hdfs")
        .setActive(true)
        .setRemoved(false);

    protected MLModelSearchSanityTest() {
        super(URN, DOCUMENT, new MLModelSearchConfig());
    }

    @Nonnull
    @Override
    public SearchIndex<MLModelDocument> getIndex() {
        return _index;
    }
}
