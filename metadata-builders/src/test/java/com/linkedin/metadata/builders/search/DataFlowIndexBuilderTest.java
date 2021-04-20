package com.linkedin.metadata.builders.search;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.metadata.aspect.DataFlowAspect;
import com.linkedin.metadata.aspect.DataFlowAspectArray;
import com.linkedin.metadata.search.DataFlowDocument;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DataFlowIndexBuilderTest {

  @Test
  public void testGetDocumentsToUpdateFromDataFlowSnapshot() {
    DataFlowUrn dataFlowUrn = new DataFlowUrn("airflow", "flow1", "main");
    DataFlowInfo dataFlowInfo = new DataFlowInfo();

    dataFlowInfo.setName("Flow number 1");
    dataFlowInfo.setDescription("A description");
    dataFlowInfo.setProject("Lost cause");

    DataFlowAspect dataFlowAspect = new DataFlowAspect();
    dataFlowAspect.setDataFlowInfo(dataFlowInfo);
    DataFlowAspectArray dataFlowAspectArray = new DataFlowAspectArray();
    dataFlowAspectArray.add(dataFlowAspect);
    DataFlowSnapshot dataFlowSnapshot = new DataFlowSnapshot().setUrn(dataFlowUrn).setAspects(dataFlowAspectArray);

    List<DataFlowDocument> actualDocs = new DataFlowIndexBuilder().getDocumentsToUpdate(dataFlowSnapshot);
    assertEquals(actualDocs.size(), 1);
    assertEquals(actualDocs.get(0).getUrn(), dataFlowUrn);
    assertEquals(actualDocs.get(0).getName(), "Flow number 1");
    assertEquals(actualDocs.get(0).getDescription(), "A description");
    assertEquals(actualDocs.get(0).getProject(), "Lost cause");
  }
}
