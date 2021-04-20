package com.linkedin.metadata.builders.search;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.metadata.aspect.DataJobAspectArray;
import com.linkedin.metadata.search.DataJobDocument;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static com.linkedin.metadata.builders.common.DataJobTestUtils.*;

public class DataJobIndexBuilderTest {

  @Test
  public void testGetDocumentsToUpdateFromDataJobSnapshot() {
    DataFlowUrn dataFlowUrn = new DataFlowUrn("airflow", "flow1", "main");
    DataJobUrn dataJobUrn = new DataJobUrn(dataFlowUrn, "you_had_one_job");

    DataJobAspectArray dataJobAspectArray = new DataJobAspectArray();
    dataJobAspectArray.add(makeDataJobInputOutputAspect());
    dataJobAspectArray.add(makeDataJobInfoAspect());
    dataJobAspectArray.add(makeOwnershipAspect());
    DataJobSnapshot dataJobSnapshot = new DataJobSnapshot().setUrn(dataJobUrn).setAspects(dataJobAspectArray);

    List<DataJobDocument> actualDocs = new DataJobIndexBuilder().getDocumentsToUpdate(dataJobSnapshot);
    assertEquals(actualDocs.size(), 4);
    assertEquals(actualDocs.get(0).getInputs().get(0).getDatasetNameEntity(), "input1");
    assertEquals(actualDocs.get(0).getInputs().get(1).getDatasetNameEntity(), "input2");
    assertEquals(actualDocs.get(0).getOutputs().get(0).getDatasetNameEntity(), "output1");
    assertEquals(actualDocs.get(0).getOutputs().get(1).getDatasetNameEntity(), "output2");
    assertEquals(actualDocs.get(0).getNumInputDatasets(), new Long(2));
    assertEquals(actualDocs.get(0).getNumOutputDatasets(), new Long(2));
    assertEquals(actualDocs.get(0).getUrn(), dataJobUrn);

    assertEquals(actualDocs.get(1).getName(), "You had one Job");
    assertEquals(actualDocs.get(1).getDescription(), "A Job for one");
    assertEquals(actualDocs.get(1).getUrn(), dataJobUrn);

    assertEquals(actualDocs.get(2).getOwners().size(), 1);
    assertEquals(actualDocs.get(2).getOwners().get(0), "fooUser");
    assertTrue(actualDocs.get(2).hasHasOwners());
    assertEquals(actualDocs.get(2).getUrn(), dataJobUrn);

    assertEquals(actualDocs.get(3).getJobId(), "you_had_one_job");
    assertEquals(actualDocs.get(3).getDataFlow(), "flow1");
    assertEquals(actualDocs.get(3).getUrn(), dataJobUrn);
  }
}
