package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.entity.CorpUserEntity;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CorpUserGraphBuilderTest {
  @Test
  public void testBuildEntity() {
    CorpuserUrn urn = new CorpuserUrn("foobar");
    CorpUserSnapshot snapshot = new CorpUserSnapshot().setUrn(urn).setAspects(new CorpUserAspectArray());
    CorpUserEntity expected =
        new CorpUserEntity().setUrn(urn).setName("foobar").setRemoved(false);

    List<? extends RecordTemplate> corpUserEntities = new CorpUserGraphBuilder().buildEntities(snapshot);

    assertEquals(corpUserEntities.size(), 1);
    assertEquals(corpUserEntities.get(0), expected);
  }

  @Test
  public void testBuilderRegistered() {
    assertEquals(RegisteredGraphBuilders.getGraphBuilder(CorpUserSnapshot.class).get().getClass(),
        CorpUserGraphBuilder.class);
  }
}
