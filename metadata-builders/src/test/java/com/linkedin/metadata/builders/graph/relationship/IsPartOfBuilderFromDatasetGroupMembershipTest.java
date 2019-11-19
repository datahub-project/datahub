package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DatasetGroupUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.datasetGroup.DatasetGroupMembership;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.relationship.IsPartOf;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class IsPartOfBuilderFromDatasetGroupMembershipTest {

  @Test
  public void testBuildRelationships() {
    DatasetGroupUrn datasetGroupUrn = makeDatasetGroupUrn("foo");
    DatasetUrn datasetUrn1 = makeDatasetUrn("bar1");
    DatasetUrn datasetUrn2 = makeDatasetUrn("bar2");
    DatasetGroupMembership membership =
        new DatasetGroupMembership().setDatasets(new DatasetUrnArray(Arrays.asList(datasetUrn1, datasetUrn2)));

    List<GraphBuilder.RelationshipUpdates> operations =
        new IsPartOfBuilderFromDatasetGroupMembership().buildRelationships(datasetGroupUrn, membership);

    assertEquals(operations.size(), 1);
    assertEquals(operations.get(0).getRelationships(),
        Arrays.asList(makeIsPartOf(datasetUrn1, datasetGroupUrn), makeIsPartOf(datasetUrn2, datasetGroupUrn)));
    assertEquals(operations.get(0).getPreUpdateOperation(),
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION);
  }

  private IsPartOf makeIsPartOf(DatasetUrn source, DatasetGroupUrn destination) {
    return new IsPartOf().setSource(source).setDestination(destination);
  }
}
