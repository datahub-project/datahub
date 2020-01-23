package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetAspectArray;
import com.linkedin.metadata.entity.DatasetEntity;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class DatasetGraphBuilderTest {
  @Test
  public void testBuildEntity() {
    DatasetUrn urn = makeDatasetUrn("foobar");
    DatasetSnapshot snapshot = new DatasetSnapshot().setUrn(urn).setAspects(new DatasetAspectArray());
    DatasetEntity expected = new DatasetEntity().setUrn(urn)
        .setName(urn.getDatasetNameEntity())
        .setPlatform(urn.getPlatformEntity())
        .setOrigin(urn.getOriginEntity());

    List<? extends RecordTemplate> datasetEntities = new DatasetGraphBuilder().buildEntities(snapshot);

    assertEquals(datasetEntities.size(), 1);
    assertEquals(datasetEntities.get(0), expected);
  }

  @Test
  public void testBuildRemovedEntity() {
    DatasetUrn urn = makeDatasetUrn("foobar");
    DatasetAspect aspect = new DatasetAspect();
    aspect.setStatus(makeStatus(true));
    DatasetSnapshot snapshot =
        new DatasetSnapshot().setUrn(urn).setAspects(new DatasetAspectArray(Collections.singleton(aspect)));
    DatasetEntity expected = new DatasetEntity().setUrn(urn)
        .setName(urn.getDatasetNameEntity())
        .setPlatform(urn.getPlatformEntity())
        .setOrigin(urn.getOriginEntity())
        .setRemoved(true);

    List<? extends RecordTemplate> datasetEntities = new DatasetGraphBuilder().buildEntities(snapshot);

    assertEquals(datasetEntities.size(), 1);
    assertEquals(datasetEntities.get(0), expected);
  }

  @Test
  public void testBuilderRegistered() {
    assertEquals(RegisteredGraphBuilders.getGraphBuilder(DatasetSnapshot.class).get().getClass(),
        DatasetGraphBuilder.class);
  }
}
