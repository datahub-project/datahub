package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EditableSchemaFieldInput;
import com.linkedin.datahub.graphql.generated.SchemaFieldDataType;
import com.linkedin.datahub.graphql.generated.UnlinkPhysicalChildInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.entity.logical.LogicalModelUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class UnlinkPhysicalChildResolverTest {

  @Test
  public void testUnlinkClearsAllChildColumnEdges() throws Exception {
    Urn childUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)");
    Urn parentUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:logical,p,PROD)");

    EditableSchemaFieldInput a = new EditableSchemaFieldInput();
    a.setFieldPath("id");
    a.setType(SchemaFieldDataType.NUMBER);
    EditableSchemaFieldInput b = new EditableSchemaFieldInput();
    b.setFieldPath("name");
    b.setType(SchemaFieldDataType.STRING);
    SchemaMetadata childSchema =
        SchemaMetadataUtils.buildSchemaMetadata(
            "c", new DataPlatformUrn("snowflake"), List.of(a, b));

    EnvelopedAspectMap map = new EnvelopedAspectMap();
    map.put(
        SCHEMA_METADATA_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(childSchema.data())));
    EntityClient client = mock(EntityClient.class);
    when(client.batchGetV2(any(), any(), anySet(), anySet()))
        .thenReturn(Map.of(childUrn, new EntityResponse().setAspects(map)));

    UnlinkPhysicalChildInput input = new UnlinkPhysicalChildInput();
    input.setChildUrn(childUrn.toString());

    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    QueryContext context = getMockAllowContext();
    OperationContext opContext = spy(context.getOperationContext());
    // The child is linked to parentUrn at dataset level and on both columns.
    AspectRetriever retriever =
        linkedRetriever(
            Map.of(
                childUrn,
                parentUrn,
                SchemaFieldUtils.generateSchemaFieldUrn(childUrn, "id"),
                SchemaFieldUtils.generateSchemaFieldUrn(parentUrn, "id"),
                SchemaFieldUtils.generateSchemaFieldUrn(childUrn, "name"),
                SchemaFieldUtils.generateSchemaFieldUrn(parentUrn, "name")),
            context.getOperationContext());
    doReturn(retriever).when(opContext).getAspectRetriever();
    when(context.getOperationContext()).thenReturn(opContext);
    when(env.getContext()).thenReturn(context);
    when(env.getArgument(eq("input"))).thenReturn(input);

    new UnlinkPhysicalChildResolver(client).get(env).get();

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(client, times(1)).batchIngestProposals(any(), captor.capture(), eq(false));
    // 1 dataset-level unlink + 2 column-level unlinks (id, name)
    assertEquals(captor.getValue().size(), 3);
  }

  @SuppressWarnings("unchecked")
  private static AspectRetriever linkedRetriever(
      Map<Urn, Urn> parents, OperationContext opContext) {
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(any(), anySet(), anySet()))
        .thenAnswer(
            invocation -> {
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : (Set<Urn>) invocation.getArgument(1)) {
                if (parents.containsKey(urn)) {
                  result.put(
                      urn,
                      Map.of(
                          LOGICAL_PARENT_ASPECT_NAME,
                          new Aspect(
                              LogicalModelUtils.createLogicalParent(parents.get(urn), opContext)
                                  .data())));
                }
              }
              return result;
            });
    return retriever;
  }
}
