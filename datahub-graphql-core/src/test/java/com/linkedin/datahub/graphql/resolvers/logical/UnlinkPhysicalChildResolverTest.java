package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class UnlinkPhysicalChildResolverTest {

  @Test
  public void testUnlinkClearsAllChildColumnEdges() throws Exception {
    Urn childUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)");

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
    when(env.getContext()).thenReturn(context);
    when(env.getArgument(eq("input"))).thenReturn(input);

    new UnlinkPhysicalChildResolver(client).get(env).get();

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(client, times(1)).batchIngestProposals(any(), captor.capture(), eq(false));
    // 1 dataset-level unlink + 2 column-level unlinks (id, name)
    assertEquals(captor.getValue().size(), 3);
  }
}
