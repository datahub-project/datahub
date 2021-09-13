package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.TagAspectArray;
import com.linkedin.metadata.entity.TagEntity;
import com.linkedin.metadata.snapshot.TagSnapshot;

import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.testing.Urns.makeTagUrn;
import static org.testng.Assert.*;

public class TagGraphBuilderTest {

    @Test
    public void testBuildEntity() {
        TagUrn urn = makeTagUrn("foo_job");
        TagSnapshot snapshot = new TagSnapshot().setUrn(urn).setAspects(new TagAspectArray());
        TagEntity expected = new TagEntity().setUrn(urn).setName(urn.getName());

        List<? extends RecordTemplate> tagEntities = new TagGraphBuilder().buildEntities(snapshot);

        assertEquals(tagEntities.size(), 1);
        assertEquals(tagEntities.get(0), expected);
    }

    @Test
    public void testBuilderRegistered() {
        assertEquals(RegisteredGraphBuilders.getGraphBuilder(TagSnapshot.class).get().getClass(),
                TagGraphBuilder.class);
    }
}
