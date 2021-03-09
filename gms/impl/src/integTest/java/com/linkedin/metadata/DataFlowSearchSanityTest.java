package com.linkedin.metadata;

import java.util.Collections;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.configs.DataFlowSearchConfig;
import com.linkedin.metadata.search.DataFlowDocument;
import com.linkedin.metadata.testing.BaseSearchSanityTests;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import javax.annotation.Nonnull;


public class DataFlowSearchSanityTest extends BaseSearchSanityTests<DataFlowDocument> {
    @SearchIndexType(DataFlowDocument.class)
    @SearchIndexSettings("/index/dataflow/settings.json")
    @SearchIndexMappings("/index/dataflow/mappings.json")
    public SearchIndex<DataFlowDocument> _index;

    private static final DataFlowUrn URN = new DataFlowUrn("airflow", "my_pipeline", "prod_cluster");
    private static final DataFlowDocument DOCUMENT = new DataFlowDocument().setUrn(URN)
        .setFlowId(URN.getFlowEntity())
        .setName(true)
        .setDescription("My pipeline!")
        .setOrchestrator(URN.getOrchestratorEntity())
        .setOwners(new StringArray("fbaggins"))
        .setCluster(URN.getClusterEntity())
        .setProject("toy_project");

    protected DataFlowSearchSanityTest() {
        super(URN, DOCUMENT, new DataFlowSearchConfig());
    }

    @Nonnull
    @Override
    public SearchIndex<DataFlowDocument> getIndex() {
        return _index;
    }
}
