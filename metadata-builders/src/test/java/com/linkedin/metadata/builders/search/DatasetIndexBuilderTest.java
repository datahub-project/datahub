package com.linkedin.metadata.builders.search;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DatasetIndexBuilderTest {

  @Test
  public void testDescriptionClearing() {
    DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);

    DatasetProperties datasetProperties = new DatasetProperties().setDescription("baz");
    DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetProperties)));
    List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);
    assertEquals(actualDocs.size(), 1);
    assertEquals(actualDocs.get(0).getUrn(), datasetUrn);
    assertEquals(actualDocs.get(0).getDescription(), "baz");

    datasetProperties = new DatasetProperties();
    datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetProperties)));
    actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);
    assertEquals(actualDocs.size(), 1);
    assertEquals(actualDocs.get(0).getUrn(), datasetUrn);
    assertEquals(actualDocs.get(0).getDescription(), "");
  }
}