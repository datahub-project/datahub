package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.MLModelAspectArray;
import com.linkedin.metadata.entity.MLModelEntity;
import com.linkedin.metadata.snapshot.MLModelSnapshot;

import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.testing.Urns.makeMLModelUrn;
import static org.testng.Assert.*;


public class MLModelGraphBuilderTest {

    @Test
    public void testBuildEntity() {
        MLModelUrn urn = makeMLModelUrn("scienceModel");
        MLModelSnapshot snapshot = new MLModelSnapshot().setUrn(urn).setAspects(new MLModelAspectArray());
        MLModelEntity expected = new MLModelEntity().setUrn(urn)
            .setName(urn.getMlModelNameEntity())
            .setPlatform(urn.getPlatformEntity())
            .setOrigin(urn.getOriginEntity());

        List<? extends RecordTemplate> mlModelEntities = new MLModelGraphBuilder().buildEntities(snapshot);

        assertEquals(mlModelEntities.size(), 1);
        assertEquals(mlModelEntities.get(0), expected);
    }

    @Test
    public void testBuilderRegistered() {
        assertEquals(RegisteredGraphBuilders.getGraphBuilder(MLModelSnapshot.class).get().getClass(),
            MLModelGraphBuilder.class);
    }
}
