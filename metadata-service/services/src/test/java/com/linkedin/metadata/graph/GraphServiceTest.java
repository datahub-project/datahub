package com.linkedin.metadata.graph;

import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class GraphServiceTest {

  private static final RelationshipFilter OUTGOING =
      newRelationshipFilter(
          new Filter().setOr(new ConjunctiveCriterionArray()), RelationshipDirection.OUTGOING);

  @Test
  public void testRemoveEdgesFromNodesDefaultDelegatesPerUrn() {
    GraphService graphService = mock(GraphService.class);
    doCallRealMethod().when(graphService).removeEdgesFromNodes(any(), any(), any());
    OperationContext opContext = mock(OperationContext.class);

    Urn dataset = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)");
    Urn field =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD),c0)");
    Map<Urn, Set<String>> sources = new LinkedHashMap<>();
    sources.put(dataset, Set.of("DownstreamOf", "IsPartOf"));
    sources.put(field, Set.of("DownstreamOf"));

    graphService.removeEdgesFromNodes(opContext, sources, OUTGOING);

    verify(graphService, times(1))
        .removeEdgesFromNode(opContext, dataset, Set.of("DownstreamOf", "IsPartOf"), OUTGOING);
    verify(graphService, times(1))
        .removeEdgesFromNode(opContext, field, Set.of("DownstreamOf"), OUTGOING);
    verify(graphService, times(2)).removeEdgesFromNode(any(), any(), any(), any());
  }

  @Test
  public void testRemoveEdgesFromNodesDefaultWithNoUrns() {
    GraphService graphService = mock(GraphService.class);
    doCallRealMethod().when(graphService).removeEdgesFromNodes(any(), any(), any());

    graphService.removeEdgesFromNodes(
        mock(OperationContext.class), Collections.emptyMap(), OUTGOING);

    verify(graphService, never()).removeEdgesFromNode(any(), any(), any(), any());
  }
}
