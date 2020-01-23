package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.relationship.ReportsTo;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class ReportsToBuilderFromCorpUserInfoTest {

  @Test
  public void testBuildRelationships() {
    CorpuserUrn reportUrn = makeCorpUserUrn("foo");
    CorpuserUrn managerUrn = makeCorpUserUrn("bar");

    // Manager exists
    CorpUserInfo corpUserInfo =
        new CorpUserInfo().setManagerUrn(managerUrn);

    List<GraphBuilder.RelationshipUpdates> operations =
        new ReportsToBuilderFromCorpUserInfo().buildRelationships(reportUrn, corpUserInfo);

    assertEquals(operations.size(), 1);
    assertEquals(operations.get(0).getRelationships(),
        Arrays.asList(makeReportsTo(reportUrn, managerUrn)));
    assertEquals(operations.get(0).getPreUpdateOperation(),
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);

    // No manager
    corpUserInfo = new CorpUserInfo();
    operations = new ReportsToBuilderFromCorpUserInfo().buildRelationships(reportUrn, corpUserInfo);
    assertEquals(operations.size(), 0);
  }

  private ReportsTo makeReportsTo(CorpuserUrn managee, CorpuserUrn manager) {
    return new ReportsTo().setSource(managee).setDestination(manager);
  }
}
