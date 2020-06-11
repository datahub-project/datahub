package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.entity.CorpUserEntity;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.builders.graph.CorpUserGraphBuilder.*;
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

  @Test
  public void testIsUserActive() {
    CorpuserUrn urn = new CorpuserUrn("foobar");

    CorpUserSnapshot snapshot = new CorpUserSnapshot().setUrn(urn).setAspects(new CorpUserAspectArray());
    assertTrue(isUserActive(snapshot));

    CorpUserAspect corpUserAspect = new CorpUserAspect();
    corpUserAspect.setCorpUserInfo(new CorpUserInfo().setActive(false));
    snapshot = new CorpUserSnapshot()
        .setUrn(urn)
        .setAspects(new CorpUserAspectArray(corpUserAspect));
    assertFalse(isUserActive(snapshot));

    snapshot.getAspects().get(0).getCorpUserInfo().setActive(true);
    assertTrue(isUserActive(snapshot));
  }
}
