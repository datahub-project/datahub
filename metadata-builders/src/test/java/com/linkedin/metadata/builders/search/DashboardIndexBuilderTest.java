package com.linkedin.metadata.builders.search;

import com.linkedin.common.AccessLevel;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.metadata.aspect.DashboardAspectArray;
import com.linkedin.metadata.search.DashboardDocument;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.builders.common.DashboardTestUtils.*;
import static org.testng.Assert.*;


public class DashboardIndexBuilderTest {

  @Test
  public void testGetDocumentsToUpdateFromDashboardSnapshot() {
    DashboardUrn urn = new DashboardUrn("Looker", "1");
    DashboardSnapshot snapshot = new DashboardSnapshot().setUrn(urn).setAspects(new DashboardAspectArray());

    snapshot.getAspects().add(makeDashboardInfoAspect());
    List<DashboardDocument> actualDocs = new DashboardIndexBuilder().getDocumentsToUpdate(snapshot);
    assertEquals(actualDocs.size(), 1);
    assertEquals(actualDocs.get(0).getUrn(), urn);
    assertEquals(actualDocs.get(0).getTitle(), "FooDashboard");
    assertEquals(actualDocs.get(0).getDescription(), "Descripton for FooDashboard");
    assertEquals(actualDocs.get(0).getTool(), "Looker");
    assertEquals(actualDocs.get(0).getAccess(), AccessLevel.PUBLIC);

    snapshot.getAspects().add(makeOwnershipAspect());
    actualDocs = new DashboardIndexBuilder().getDocumentsToUpdate(snapshot);
    assertEquals(actualDocs.size(), 2);
    assertEquals(actualDocs.get(1).getOwners().size(), 1);
    assertEquals(actualDocs.get(1).getOwners().get(0), "fooUser");

    snapshot.getAspects().add(makeStatusAspect());
    actualDocs = new DashboardIndexBuilder().getDocumentsToUpdate(snapshot);
    assertEquals(actualDocs.size(), 3);
    assertTrue(actualDocs.get(2).isRemoved());
  }
}
