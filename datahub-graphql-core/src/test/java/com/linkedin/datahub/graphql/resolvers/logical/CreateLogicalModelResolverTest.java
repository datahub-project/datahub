package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateLogicalModelInput;
import com.linkedin.datahub.graphql.generated.EditableSchemaFieldInput;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.SchemaFieldDataType;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class CreateLogicalModelResolverTest {

  private static EntityResponse dataPlatformInfoResponse(boolean logical) {
    final DataPlatformInfo info =
        new DataPlatformInfo()
            .setName("snowflake")
            .setType(com.linkedin.dataplatform.PlatformType.OTHERS)
            .setDatasetNameDelimiter(".")
            .setLogical(logical);
    final EnvelopedAspect envelopedAspect =
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(info.data()));
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(DATA_PLATFORM_INFO_ASPECT_NAME, envelopedAspect);
    return new EntityResponse()
        .setUrn(UrnUtils.getUrn("urn:li:dataPlatform:snowflake"))
        .setEntityName("dataPlatform")
        .setAspects(aspectMap);
  }

  private EditableSchemaFieldInput col(String path) {
    EditableSchemaFieldInput c = new EditableSchemaFieldInput();
    c.setFieldPath(path);
    c.setType(SchemaFieldDataType.NUMBER);
    return c;
  }

  private CreateLogicalModelInput inputWithColumns(List<EditableSchemaFieldInput> cols) {
    CreateLogicalModelInput input = new CreateLogicalModelInput();
    input.setName("my_domain.my_table");
    input.setEnv(FabricType.PROD);
    input.setColumns(cols);
    return input;
  }

  private CreateLogicalModelInput input() {
    return inputWithColumns(List.of(col("id")));
  }

  private DataFetchingEnvironment env(QueryContext context, CreateLogicalModelInput input) {
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getArgument(eq("input"))).thenReturn(input);
    return env;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCreateLogicalModelSuccess() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.exists(any(), any())).thenReturn(false);
    // The default "logical" platform already has a DataPlatformInfo{logical:true} seeded.
    when(mockClient.getV2(any(), eq("dataPlatform"), any(), any()))
        .thenReturn(dataPlatformInfoResponse(true));

    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    String urn = resolver.get(env(getMockAllowContext(), input())).get();
    assertEquals(urn, "urn:li:dataset:(urn:li:dataPlatform:logical,my_domain.my_table,PROD)");

    // properties+key and schemaMetadata MCPs in a single batch; no subtype, no platform-info MCP
    // since the platform already exists as logical.
    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockClient, times(1)).batchIngestProposals(any(), captor.capture(), eq(false));
    verify(mockClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
    assertEquals(captor.getValue().size(), 2);
  }

  @Test
  public void testDoesNotStampLogicalModelSubtype() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.exists(any(), any())).thenReturn(false);
    when(mockClient.getV2(any(), eq("dataPlatform"), any(), any()))
        .thenReturn(dataPlatformInfoResponse(true));
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    resolver.get(env(getMockAllowContext(), input())).get();

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockClient).batchIngestProposals(any(), captor.capture(), eq(false));
    final boolean hasSubTypesProposal =
        captor.getValue().stream().anyMatch(mcp -> "subTypes".equals(mcp.getAspectName()));
    assertFalse(hasSubTypesProposal);
  }

  @Test
  public void testCreatesPlatformInfoWhenPlatformIsNew() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.exists(any(), any())).thenReturn(false);
    // No DataPlatformInfo aspect exists yet for this custom platform.
    when(mockClient.getV2(any(), eq("dataPlatform"), any(), any())).thenReturn(null);

    CreateLogicalModelInput input = input();
    input.setPlatform("urn:li:dataPlatform:my_platform");
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    resolver.get(env(getMockAllowContext(), input)).get();

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockClient).batchIngestProposals(any(), captor.capture(), eq(false));
    final List<MetadataChangeProposal> proposals = captor.getValue();
    assertEquals(proposals.size(), 3);
    final MetadataChangeProposal platformInfoProposal =
        proposals.stream()
            .filter(mcp -> DATA_PLATFORM_INFO_ASPECT_NAME.equals(mcp.getAspectName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected a dataPlatformInfo MCP"));
    final DataPlatformInfo platformInfo =
        com.linkedin.metadata.utils.GenericRecordUtils.deserializeAspect(
            platformInfoProposal.getAspect().getValue(),
            platformInfoProposal.getAspect().getContentType(),
            DataPlatformInfo.class);
    assertTrue(Boolean.TRUE.equals(platformInfo.isLogical()));
    assertEquals(platformInfo.getName(), "my_platform");
  }

  @Test
  public void testRejectsOverlongNewPlatformName() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.exists(any(), any())).thenReturn(false);
    when(mockClient.getV2(any(), eq("dataPlatform"), any(), any())).thenReturn(null);

    CreateLogicalModelInput input = input();
    input.setPlatform("urn:li:dataPlatform:way_too_long_platform_name");
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    assertThrows(
        CompletionException.class, () -> resolver.get(env(getMockAllowContext(), input)).join());
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testRejectsNonLogicalExistingPlatform() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.exists(any(), any())).thenReturn(false);
    when(mockClient.getV2(any(), eq("dataPlatform"), any(), any()))
        .thenReturn(dataPlatformInfoResponse(false));

    CreateLogicalModelInput input = input();
    input.setPlatform("urn:li:dataPlatform:snowflake");
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    assertThrows(
        CompletionException.class, () -> resolver.get(env(getMockAllowContext(), input)).join());
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testAllowsSameNameOnDifferentPlatforms() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    // The name exists on the default "logical" platform, but not on this one — dedup is keyed
    // on the full dataset URN (platform+name+env), not the bare name.
    when(mockClient.exists(
            any(),
            eq(
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:logical,my_domain.my_table,PROD)"))))
        .thenReturn(true);
    when(mockClient.exists(
            any(),
            eq(
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:other_logical,my_domain.my_table,PROD)"))))
        .thenReturn(false);
    when(mockClient.getV2(any(), eq("dataPlatform"), any(), any()))
        .thenReturn(dataPlatformInfoResponse(true));

    CreateLogicalModelInput input = input();
    input.setPlatform("urn:li:dataPlatform:other_logical");
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    String urn = resolver.get(env(getMockAllowContext(), input)).get();
    assertEquals(urn, "urn:li:dataset:(urn:li:dataPlatform:other_logical,my_domain.my_table,PROD)");
  }

  @Test
  public void testCreateLogicalModelUnauthorized() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    assertThrows(
        CompletionException.class, () -> resolver.get(env(getMockDenyContext(), input())).join());
  }

  @Test
  public void testCreateLogicalModelAlreadyExists() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.exists(any(), any())).thenReturn(true);
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    assertThrows(
        CompletionException.class, () -> resolver.get(env(getMockAllowContext(), input())).join());
  }

  @Test
  public void testCreateLogicalModelRejectsEmptyColumns() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.exists(any(), any())).thenReturn(false);
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    assertThrows(
        CompletionException.class,
        () -> resolver.get(env(getMockAllowContext(), inputWithColumns(List.of()))).join());
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testCreateLogicalModelRejectsDuplicateColumns() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.exists(any(), any())).thenReturn(false);
    CreateLogicalModelResolver resolver = new CreateLogicalModelResolver(mockClient);

    assertThrows(
        CompletionException.class,
        () ->
            resolver
                .get(env(getMockAllowContext(), inputWithColumns(List.of(col("id"), col("id")))))
                .join());
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }
}
