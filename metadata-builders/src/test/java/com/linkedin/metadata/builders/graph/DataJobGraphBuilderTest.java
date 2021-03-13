package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.DataJobAspectArray;
import com.linkedin.metadata.entity.DataJobEntity;
import com.linkedin.metadata.snapshot.DataJobSnapshot;

import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.testing.Urns.makeDataJobUrn;
import static org.testng.Assert.*;


public class DataJobGraphBuilderTest {

    @Test
    public void testBuildEntity() {
        DataJobUrn urn = makeDataJobUrn("foo_job");
        DataJobSnapshot snapshot = new DataJobSnapshot().setUrn(urn).setAspects(new DataJobAspectArray());
        DataJobEntity expected = new DataJobEntity().setUrn(urn)
            .setFlow(urn.getFlowEntity())
            .setJobId(urn.getJobIdEntity());

        List<? extends RecordTemplate> dataJobEntities = new DataJobGraphBuilder().buildEntities(snapshot);

        assertEquals(dataJobEntities.size(), 1);
        assertEquals(dataJobEntities.get(0), expected);
    }

    @Test
    public void testBuilderRegistered() {
        assertEquals(RegisteredGraphBuilders.getGraphBuilder(DataJobSnapshot.class).get().getClass(),
            DataJobGraphBuilder.class);
    }
}
