package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.DataProcessAspectArray;
import com.linkedin.metadata.entity.DataProcessEntity;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class DataProcessGraphBuilderTest {

  @Test
  public void testBuildEntity() {
    DataProcessUrn urn = makeDataProcessUrn("Sqoop ETL");
    DataProcessSnapshot snapshot = new DataProcessSnapshot().setUrn(urn).setAspects(new DataProcessAspectArray());
    DataProcessEntity expected = new DataProcessEntity().setUrn(urn)
        .setName(urn.getNameEntity())
        .setOrchestrator(urn.getOrchestrator())
        .setOrigin(urn.getOriginEntity());

    List<? extends RecordTemplate> dataProcessEntities = new DataProcessGraphBuilder().buildEntities(snapshot);

    assertEquals(dataProcessEntities.size(), 1);
    assertEquals(dataProcessEntities.get(0), expected);
  }

  @Test
  public void testBuilderRegistered() {
    assertEquals(RegisteredGraphBuilders.getGraphBuilder(DataProcessSnapshot.class).get().getClass(),
        DataProcessGraphBuilder.class);
  }
}
