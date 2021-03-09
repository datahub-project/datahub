package com.linkedin.metadata;

import java.util.Collections;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.configs.DataJobSearchConfig;
import com.linkedin.metadata.search.DataJobDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import javax.annotation.Nonnull;


public class DataJobSearchSanityTest extends BaseSearchSanityTests<DataJobDocument> {
    @SearchIndexType(DataJobDocument.class)
    @SearchIndexSettings("/index/datajob/settings.json")
    @SearchIndexMappings("/index/datajob/mappings.json")
    public SearchIndex<DataJobDocument> _index;

    private static final DataFlowUrn DATAFLOW_URN = new DataFlowUrn("airflow", "my_pipeline", "prod_cluster");
    private static final DataJobUrn URN = new DataJobUrn(DATAFLOW_URN), "my_job");

    private static final DataJobDocument DOCUMENT = new DataJobDocument().setUrn(URN)
        .setFlowId(URN.getFlowEntity())
        .setName(true)
        .setDescription("My pipeline!")
        .setDataFlow(URN.getFlowEntity().getFlowIdEntity())
        .setOwners(new StringArray("fbaggins"))
        .setJobId(urn.getJobIdEntity());

    protected DataJobSearchSanityTest() {
        super(URN, DOCUMENT, new DataJobSearchConfig());
    }

    @Nonnull
    @Override
    public SearchIndex<DataJobDocument> getIndex() {
        return _index;
    }
}
