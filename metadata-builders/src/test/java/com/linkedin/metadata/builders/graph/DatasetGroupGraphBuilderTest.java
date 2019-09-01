package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DatasetGroupUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.DatasetGroupAspectArray;
import com.linkedin.metadata.entity.DatasetGroupEntity;
import com.linkedin.metadata.snapshot.DatasetGroupSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DatasetGroupGraphBuilderTest {

  @Test
  public void testBuildEntity() {
    DatasetGroupUrn urn = new DatasetGroupUrn("foo", "bar");
    DatasetGroupSnapshot snapshot = new DatasetGroupSnapshot().setUrn(urn).setAspects(new DatasetGroupAspectArray());
    DatasetGroupEntity expected =
        new DatasetGroupEntity().setUrn(urn).setNamespace("foo").setName("bar").setRemoved(false);

    List<? extends RecordTemplate> datasetGroups = new DatasetGroupGraphBuilder().buildEntities(snapshot);

    assertEquals(datasetGroups.size(), 1);
    assertEquals(datasetGroups.get(0), expected);
  }

  @Test
  public void testBuilderRegistered() {
    assertEquals(RegisteredGraphBuilders.getGraphBuilder(DatasetGroupSnapshot.class).get().getClass(),
        DatasetGroupGraphBuilder.class);
  }
}
