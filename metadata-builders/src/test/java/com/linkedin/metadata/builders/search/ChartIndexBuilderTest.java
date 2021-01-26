package com.linkedin.metadata.builders.search;

import com.linkedin.chart.ChartQueryType;
import com.linkedin.chart.ChartType;
import com.linkedin.common.AccessLevel;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.metadata.aspect.ChartAspectArray;
import com.linkedin.metadata.search.ChartDocument;
import com.linkedin.metadata.snapshot.ChartSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.builders.common.ChartTestUtils.*;
import static org.testng.Assert.*;


public class ChartIndexBuilderTest {

  @Test
  public void testGetDocumentsToUpdateFromChartSnapshot() {
    ChartUrn urn = new ChartUrn("Looker", "1");
    ChartSnapshot snapshot = new ChartSnapshot().setUrn(urn).setAspects(new ChartAspectArray());

    snapshot.getAspects().add(makeChartInfoAspect());
    List<ChartDocument> actualDocs = new ChartIndexBuilder().getDocumentsToUpdate(snapshot);
    assertEquals(actualDocs.size(), 1);
    assertEquals(actualDocs.get(0).getUrn(), urn);
    assertEquals(actualDocs.get(0).getTitle(), "FooChart");
    assertEquals(actualDocs.get(0).getDescription(), "Descripton for FooChart");
    assertEquals(actualDocs.get(0).getTool(), "Looker");
    assertEquals(actualDocs.get(0).getType(), ChartType.PIE);
    assertEquals(actualDocs.get(0).getAccess(), AccessLevel.PUBLIC);

    snapshot.getAspects().add(makeChartQueryAspect());
    actualDocs = new ChartIndexBuilder().getDocumentsToUpdate(snapshot);
    assertEquals(actualDocs.size(), 2);
    assertEquals(actualDocs.get(1).getQueryType(), ChartQueryType.LOOKML);

    snapshot.getAspects().add(makeOwnershipAspect());
    actualDocs = new ChartIndexBuilder().getDocumentsToUpdate(snapshot);
    assertEquals(actualDocs.size(), 3);
    assertEquals(actualDocs.get(2).getOwners().size(), 1);
    assertEquals(actualDocs.get(2).getOwners().get(0), "fooUser");

    snapshot.getAspects().add(makeStatusAspect());
    actualDocs = new ChartIndexBuilder().getDocumentsToUpdate(snapshot);
    assertEquals(actualDocs.size(), 4);
    assertTrue(actualDocs.get(3).isRemoved());
  }
}
