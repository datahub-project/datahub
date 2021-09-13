package com.linkedin.metadata.builders.search;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.metadata.aspect.DataFlowAspectArray;
import com.linkedin.metadata.search.DataFlowDocument;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static com.linkedin.metadata.builders.common.DataFlowTestUtils.*;

public class DataFlowIndexBuilderTest {

  @Test
  public void testGetDocumentsToUpdateFromDataFlowSnapshot() {
    DataFlowUrn dataFlowUrn = new DataFlowUrn("airflow", "flow1", "main");
    DataFlowInfo dataFlowInfo = new DataFlowInfo();

    DataFlowAspectArray dataFlowAspectArray = new DataFlowAspectArray();
    dataFlowAspectArray.add(makeDataFlowInfoAspect());
    dataFlowAspectArray.add(makeOwnershipAspect());
    DataFlowSnapshot dataFlowSnapshot = new DataFlowSnapshot().setUrn(dataFlowUrn).setAspects(dataFlowAspectArray);

    List<DataFlowDocument> actualDocs = new DataFlowIndexBuilder().getDocumentsToUpdate(dataFlowSnapshot);
    assertEquals(actualDocs.size(), 3);

    assertEquals(actualDocs.get(0).getName(), "Flow number 1");
    assertEquals(actualDocs.get(0).getDescription(), "A description");
    assertEquals(actualDocs.get(0).getProject(), "Lost cause");
    assertEquals(actualDocs.get(0).getUrn(), dataFlowUrn);

    assertEquals(actualDocs.get(1).getOwners().size(), 1);
    assertEquals(actualDocs.get(1).getOwners().get(0), "fooUser");
    assertTrue(actualDocs.get(1).hasHasOwners());
    assertEquals(actualDocs.get(1).getUrn(), dataFlowUrn);

    assertEquals(actualDocs.get(2).getOrchestrator(), "airflow");
    assertEquals(actualDocs.get(2).getFlowId(), "flow1");
    assertEquals(actualDocs.get(2).getCluster(), "main");
    assertEquals(actualDocs.get(2).getUrn(), dataFlowUrn);
  }
}
