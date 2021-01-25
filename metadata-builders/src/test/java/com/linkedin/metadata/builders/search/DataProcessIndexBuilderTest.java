package com.linkedin.metadata.builders.search;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataprocess.DataProcessInfo;
import com.linkedin.metadata.aspect.DataProcessAspect;
import com.linkedin.metadata.aspect.DataProcessAspectArray;
import com.linkedin.metadata.search.DataProcessDocument;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DataProcessIndexBuilderTest {

  @Test
  public void testGetDocumentsToUpdateFromDataProcessSnapshot() {
    DataProcessUrn dataProcessUrn = new DataProcessUrn("Azure Data Factory", "ADFJob1", FabricType.PROD);
    DataProcessInfo dataProcessInfo = new DataProcessInfo();

    DatasetUrn inputDatasetUrn = new DatasetUrn(new DataPlatformUrn("HIVE"), "SampleInputDataset", FabricType.DEV);
    DatasetUrnArray inputs = new DatasetUrnArray();
    inputs.add(inputDatasetUrn);
    dataProcessInfo.setInputs(inputs);
    DatasetUrn outputDatasetUrn = new DatasetUrn(new DataPlatformUrn("HIVE"), "SampleOutputDataset", FabricType.DEV);
    DatasetUrnArray outputs = new DatasetUrnArray();
    outputs.add(outputDatasetUrn);
    dataProcessInfo.setOutputs(outputs);

    DataProcessAspect dataProcessAspect = new DataProcessAspect();
    dataProcessAspect.setDataProcessInfo(dataProcessInfo);
    DataProcessAspectArray dataProcessAspectArray = new DataProcessAspectArray();
    dataProcessAspectArray.add(dataProcessAspect);
    DataProcessSnapshot dataProcessSnapshot =
        new DataProcessSnapshot().setUrn(dataProcessUrn).setAspects(dataProcessAspectArray);

    List<DataProcessDocument> actualDocs = new DataProcessIndexBuilder().getDocumentsToUpdate(dataProcessSnapshot);
    assertEquals(actualDocs.size(), 2);
    assertEquals(actualDocs.get(0).getInputs().get(0), inputDatasetUrn);
    assertEquals(actualDocs.get(0).getOutputs().get(0), outputDatasetUrn);
    assertEquals(actualDocs.get(0).getUrn(), dataProcessUrn);
    assertEquals(actualDocs.get(1).getUrn(), dataProcessUrn);
  }
}
