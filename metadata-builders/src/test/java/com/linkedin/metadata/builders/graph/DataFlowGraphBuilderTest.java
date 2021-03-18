package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.DataFlowAspectArray;
import com.linkedin.metadata.entity.DataFlowEntity;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;

import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.testing.Urns.makeDataFlowUrn;
import static org.testng.Assert.*;


public class DataFlowGraphBuilderTest {

    @Test
    public void testBuildEntity() {
        DataFlowUrn urn = makeDataFlowUrn("etl_pipeline");
        DataFlowSnapshot snapshot = new DataFlowSnapshot().setUrn(urn).setAspects(new DataFlowAspectArray());
        DataFlowEntity expected = new DataFlowEntity().setUrn(urn)
            .setOrchestrator(urn.getOrchestratorEntity())
            .setFlowId(urn.getFlowIdEntity())
            .setCluster(urn.getClusterEntity());

        List<? extends RecordTemplate> dataFlowEntities = new DataFlowGraphBuilder().buildEntities(snapshot);

        assertEquals(dataFlowEntities.size(), 1);
        assertEquals(dataFlowEntities.get(0), expected);
    }

    @Test
    public void testBuilderRegistered() {
        assertEquals(RegisteredGraphBuilders.getGraphBuilder(DataFlowSnapshot.class).get().getClass(),
            DataFlowGraphBuilder.class);
    }
}
