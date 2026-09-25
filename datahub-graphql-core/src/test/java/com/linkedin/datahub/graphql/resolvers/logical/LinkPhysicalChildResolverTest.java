package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ColumnMappingInput;
import com.linkedin.datahub.graphql.generated.LinkPhysicalChildInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class LinkPhysicalChildResolverTest {

  private static final String PARENT = "urn:li:dataset:(urn:li:dataPlatform:logical,p,PROD)";
  private static final String CHILD = "urn:li:dataset:(urn:li:dataPlatform:snowflake,c,PROD)";

  private LinkPhysicalChildInput input() {
    ColumnMappingInput m = new ColumnMappingInput();
    m.setParentFieldPath("id");
    m.setChildFieldPath("ID");
    LinkPhysicalChildInput input = new LinkPhysicalChildInput();
    input.setLogicalParentUrn(PARENT);
    input.setChildUrn(CHILD);
    input.setColumnMappings(List.of(m));
    return input;
  }

  @Test
  public void testLinkSuccess() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    LinkPhysicalChildResolver resolver = new LinkPhysicalChildResolver(mockClient);
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    QueryContext context = getMockAllowContext();
    when(env.getContext()).thenReturn(context);
    when(env.getArgument("input")).thenReturn(input());

    assertTrue(resolver.get(env).get());
    verify(mockClient, atLeastOnce()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testLinkWiresChildAndParentAndColumnMapping() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    LinkPhysicalChildResolver resolver = new LinkPhysicalChildResolver(mockClient);
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    QueryContext context = getMockAllowContext();
    when(env.getContext()).thenReturn(context);
    when(env.getArgument("input")).thenReturn(input());

    assertTrue(resolver.get(env).get());

    ArgumentCaptor<List<MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockClient, atLeastOnce())
        .batchIngestProposals(any(), proposalsCaptor.capture(), anyBoolean());
    List<MetadataChangeProposal> proposals = proposalsCaptor.getValue();

    // Dataset-level edge must run CHILD -> PARENT, not the reverse: a resolver bug that swaps the
    // GraphQL input's child/parent would either point this proposal's entity at PARENT or its
    // destinationUrn at CHILD.
    MetadataChangeProposal datasetProposal =
        proposals.stream().filter(p -> CHILD.equals(p.getEntityUrn().toString())).findFirst().get();
    LogicalParent datasetLogicalParent =
        GenericRecordUtils.deserializeAspect(
            datasetProposal.getAspect().getValue(),
            datasetProposal.getAspect().getContentType(),
            LogicalParent.class);
    assertEquals(datasetLogicalParent.getParent().getDestinationUrn().toString(), PARENT);

    // Column-level edge must carry the input's mapping (parentFieldPath="id" ->
    // childFieldPath="ID"),
    // not a dropped or mismatched mapping.
    Urn parentFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(UrnUtils.getUrn(PARENT), "id");
    Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(UrnUtils.getUrn(CHILD), "ID");
    MetadataChangeProposal columnProposal =
        proposals.stream()
            .filter(
                p ->
                    LOGICAL_PARENT_ASPECT_NAME.equals(p.getAspectName())
                        && childFieldUrn.toString().equals(p.getEntityUrn().toString()))
            .findFirst()
            .get();
    LogicalParent columnLogicalParent =
        GenericRecordUtils.deserializeAspect(
            columnProposal.getAspect().getValue(),
            columnProposal.getAspect().getContentType(),
            LogicalParent.class);
    assertEquals(
        columnLogicalParent.getParent().getDestinationUrn().toString(), parentFieldUrn.toString());
  }
}
