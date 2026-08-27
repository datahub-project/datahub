package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContextWithOperationContext;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
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

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EditableSchemaFieldInput;
import com.linkedin.datahub.graphql.generated.SchemaFieldDataType;
import com.linkedin.datahub.graphql.generated.UpdateLogicalModelSchemaInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class UpdateLogicalModelSchemaResolverTest {

  private static final String MODEL_URN =
      "urn:li:dataset:(urn:li:dataPlatform:logical,my_domain.my_table,PROD)";
  private static final Urn OTHER_MODEL_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:logical,other.model,PROD)");
  private static final DataPlatformUrn PLATFORM = new DataPlatformUrn("logical");
  private static final DataPlatformUrn PHYSICAL_PLATFORM = new DataPlatformUrn("snowflake");
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:test");

  private EditableSchemaFieldInput col(String path, SchemaFieldDataType type) {
    EditableSchemaFieldInput c = new EditableSchemaFieldInput();
    c.setFieldPath(path);
    c.setType(type);
    return c;
  }

  private UpdateLogicalModelSchemaInput input(List<EditableSchemaFieldInput> cols) {
    UpdateLogicalModelSchemaInput i = new UpdateLogicalModelSchemaInput();
    i.setUrn(MODEL_URN);
    i.setColumns(cols);
    return i;
  }

  private EntityResponse responseWith(SchemaMetadata schema) {
    EnvelopedAspect env = new EnvelopedAspect().setValue(new Aspect(schema.data()));
    EnvelopedAspectMap map = new EnvelopedAspectMap();
    map.put(SCHEMA_METADATA_ASPECT_NAME, env);
    return new EntityResponse().setAspects(map);
  }

  /** A physical-child schema with the given child field paths (all NUMBER-typed). */
  private SchemaMetadata childSchema(List<String> childFieldPaths) {
    List<EditableSchemaFieldInput> cols = new ArrayList<>();
    for (String path : childFieldPaths) {
      cols.add(col(path, SchemaFieldDataType.NUMBER));
    }
    return SchemaMetadataUtils.buildSchemaMetadata("c", PHYSICAL_PLATFORM, cols);
  }

  /** A schemaField entity response carrying a logicalParent edge to {@code parentFieldUrn}. */
  private EntityResponse logicalParentResponse(Urn parentFieldUrn) {
    Edge edge =
        new Edge()
            .setDestinationUrn(parentFieldUrn)
            .setCreated(new AuditStamp().setTime(1L).setActor(ACTOR))
            .setLastModified(new AuditStamp().setTime(1L).setActor(ACTOR));
    LogicalParent aspect = new LogicalParent().setParent(edge);
    EnvelopedAspect env = new EnvelopedAspect().setValue(new Aspect(aspect.data()));
    EnvelopedAspectMap map = new EnvelopedAspectMap();
    map.put(LOGICAL_PARENT_ASPECT_NAME, env);
    return new EntityResponse().setAspects(map);
  }

  /**
   * Stubs the batched schema and schema-field reads for a set of physical children. {@code
   * childMappings} maps each child URN to its (childFieldPath -> parentFieldPath) column mappings
   * pointing at MODEL_URN.
   */
  @SuppressWarnings("unchecked")
  private void stubChildMappings(EntityClient client, Map<Urn, Map<String, String>> childMappings)
      throws Exception {
    stubChildMappings(client, childMappings, Map.of());
  }

  /**
   * Like {@link #stubChildMappings(EntityClient, Map)}, but also stubs, per child, extra
   * schema-field logicalParent edges pointing at an {@code otherParentMappings} parent dataset
   * other than MODEL_URN. Used to verify that mappings to unrelated logical parents survive a
   * breaking edit on MODEL_URN.
   */
  @SuppressWarnings("unchecked")
  private void stubChildMappings(
      EntityClient client,
      Map<Urn, Map<String, String>> childMappings,
      Map<Urn, Map<String, String>> otherParentMappings)
      throws Exception {
    Map<Urn, EntityResponse> datasetResponses = new HashMap<>();
    Map<Urn, EntityResponse> fieldResponses = new HashMap<>();
    Urn modelUrn = UrnUtils.getUrn(MODEL_URN);
    for (Map.Entry<Urn, Map<String, String>> entry : childMappings.entrySet()) {
      Urn childUrn = entry.getKey();
      Map<String, String> otherMapping = otherParentMappings.getOrDefault(childUrn, Map.of());
      List<String> childFieldPaths = new ArrayList<>(entry.getValue().keySet());
      childFieldPaths.addAll(otherMapping.keySet());
      datasetResponses.put(childUrn, responseWith(childSchema(childFieldPaths)));
      for (Map.Entry<String, String> mapping : entry.getValue().entrySet()) {
        Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(childUrn, mapping.getKey());
        Urn parentFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(modelUrn, mapping.getValue());
        fieldResponses.put(childFieldUrn, logicalParentResponse(parentFieldUrn));
      }
      for (Map.Entry<String, String> mapping : otherMapping.entrySet()) {
        Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(childUrn, mapping.getKey());
        Urn otherParentFieldUrn =
            SchemaFieldUtils.generateSchemaFieldUrn(OTHER_MODEL_URN, mapping.getValue());
        fieldResponses.put(childFieldUrn, logicalParentResponse(otherParentFieldUrn));
      }
    }
    when(client.batchGetV2(
            any(), eq(DATASET_ENTITY_NAME), anySet(), eq(Set.of(SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(datasetResponses);
    when(client.batchGetV2(
            any(), eq(SCHEMA_FIELD_ENTITY_NAME), anySet(), eq(Set.of(LOGICAL_PARENT_ASPECT_NAME))))
        .thenReturn(fieldResponses);
  }

  private EntityRelationships page(List<Urn> children, int start, int total) {
    EntityRelationshipArray rels = new EntityRelationshipArray();
    for (Urn child : children) {
      rels.add(new EntityRelationship().setEntity(child).setType("PhysicalInstanceOf"));
    }
    return new EntityRelationships()
        .setStart(start)
        .setCount(children.size())
        .setTotal(total)
        .setRelationships(rels);
  }

  private DataFetchingEnvironment env(QueryContext context, UpdateLogicalModelSchemaInput input) {
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getArgument(eq("input"))).thenReturn(input);
    return env;
  }

  /** True if the batch clears the child dataset-level logicalParent for {@code childUrn}. */
  private boolean clearsDatasetLink(List<MetadataChangeProposal> proposals, Urn childUrn) {
    return proposals.stream()
        .anyMatch(
            p ->
                childUrn.equals(p.getEntityUrn()) && DATASET_ENTITY_NAME.equals(p.getEntityType()));
  }

  /** True if the batch clears the child schema-field logicalParent for {@code childFieldPath}. */
  private boolean clearsColumnEdge(
      List<MetadataChangeProposal> proposals, Urn childUrn, String childFieldPath) {
    Urn fieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(childUrn, childFieldPath);
    return proposals.stream()
        .anyMatch(
            p ->
                fieldUrn.equals(p.getEntityUrn())
                    && LOGICAL_PARENT_ASPECT_NAME.equals(p.getAspectName()));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNonBreakingAddReEmitsSchemaOnly() throws Exception {
    SchemaMetadata existing =
        SchemaMetadataUtils.buildSchemaMetadata(
            "s", PLATFORM, List.of(col("id", SchemaFieldDataType.NUMBER)));
    EntityClient client = mock(EntityClient.class);
    GraphClient graph = mock(GraphClient.class);
    when(client.getV2(any(), anyString(), any(Urn.class), anySet()))
        .thenReturn(responseWith(existing));

    UpdateLogicalModelSchemaResolver resolver = new UpdateLogicalModelSchemaResolver(client, graph);
    resolver
        .get(
            env(
                getMockAllowContext(),
                input(
                    List.of(
                        col("id", SchemaFieldDataType.NUMBER),
                        col("name", SchemaFieldDataType.STRING)))))
        .get();

    // A non-breaking change writes only the schema (no children enumerated, no teardown).
    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(client, never()).ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
    verify(client, times(1)).batchIngestProposals(any(), captor.capture(), eq(false));
    verify(graph, never())
        .getRelatedEntities(anyString(), anySet(), any(), anyInt(), anyInt(), anyString());

    List<MetadataChangeProposal> sent = captor.getValue();
    assertEquals(sent.size(), 1);
    assertEquals(sent.get(0).getAspectName(), SCHEMA_METADATA_ASPECT_NAME);
    assertEquals(sent.get(0).getEntityUrn(), UrnUtils.getUrn(MODEL_URN));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBreakingEditClearsOnlyAffectedChildMappings() throws Exception {
    // Parent has columns a (NUMBER) and b (STRING); the edit drops "a".
    SchemaMetadata existing =
        SchemaMetadataUtils.buildSchemaMetadata(
            "s",
            PLATFORM,
            List.of(col("a", SchemaFieldDataType.NUMBER), col("b", SchemaFieldDataType.STRING)));
    Urn childUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)");

    EntityClient client = mock(EntityClient.class);
    GraphClient graph = mock(GraphClient.class);
    when(client.getV2(any(), anyString(), eq(UrnUtils.getUrn(MODEL_URN)), anySet()))
        .thenReturn(responseWith(existing));
    // Child maps {a->x, b->y}. Dropping "a" affects only the a->x edge.
    Map<String, String> mapping = new HashMap<>();
    mapping.put("x", "a");
    mapping.put("y", "b");
    stubChildMappings(client, Map.of(childUrn, mapping));
    when(graph.getRelatedEntities(anyString(), anySet(), any(), anyInt(), anyInt(), anyString()))
        .thenReturn(page(List.of(childUrn), 0, 1));

    UpdateLogicalModelSchemaResolver resolver = new UpdateLogicalModelSchemaResolver(client, graph);
    resolver
        .get(env(getMockAllowContext(), input(List.of(col("b", SchemaFieldDataType.STRING)))))
        .get();

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(client, times(1)).batchIngestProposals(any(), captor.capture(), eq(false));
    List<MetadataChangeProposal> sent = captor.getValue();

    // The affected column edge (x -> a) is cleared; the surviving edge (y -> b) and the
    // dataset-level link are left intact. Schema is written last.
    assertTrue(clearsColumnEdge(sent, childUrn, "x"), "expected a->x edge cleared");
    assertFalse(clearsColumnEdge(sent, childUrn, "y"), "expected b->y edge preserved");
    assertFalse(clearsDatasetLink(sent, childUrn), "expected dataset-level link preserved");
    MetadataChangeProposal last = sent.get(sent.size() - 1);
    assertEquals(last.getAspectName(), SCHEMA_METADATA_ASPECT_NAME);
    assertEquals(last.getEntityUrn(), UrnUtils.getUrn(MODEL_URN));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBreakingEditPreservesMappingsToOtherLogicalParents() throws Exception {
    // Parent has columns a (NUMBER) and b (STRING); the edit drops "a".
    SchemaMetadata existing =
        SchemaMetadataUtils.buildSchemaMetadata(
            "s",
            PLATFORM,
            List.of(col("a", SchemaFieldDataType.NUMBER), col("b", SchemaFieldDataType.STRING)));
    Urn childUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)");

    EntityClient client = mock(EntityClient.class);
    GraphClient graph = mock(GraphClient.class);
    when(client.getV2(any(), anyString(), eq(UrnUtils.getUrn(MODEL_URN)), anySet()))
        .thenReturn(responseWith(existing));
    // Child maps {x->a, y->b} to MODEL_URN and, separately, {z->a} to an unrelated logical parent
    // that happens to also define a column literally named "a". Dropping "a" from MODEL_URN must
    // clear only x->a; the surviving y->b edge and the z->a edge (which resolves against a
    // different logical model entirely, despite the coincidental same field-path name) must be
    // left untouched.
    Map<String, String> modelMapping = new HashMap<>();
    modelMapping.put("x", "a");
    modelMapping.put("y", "b");
    stubChildMappings(client, Map.of(childUrn, modelMapping), Map.of(childUrn, Map.of("z", "a")));
    when(graph.getRelatedEntities(anyString(), anySet(), any(), anyInt(), anyInt(), anyString()))
        .thenReturn(page(List.of(childUrn), 0, 1));

    UpdateLogicalModelSchemaResolver resolver = new UpdateLogicalModelSchemaResolver(client, graph);
    resolver
        .get(env(getMockAllowContext(), input(List.of(col("b", SchemaFieldDataType.STRING)))))
        .get();

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(client, times(1)).batchIngestProposals(any(), captor.capture(), eq(false));
    List<MetadataChangeProposal> sent = captor.getValue();

    // Only the affected edge to MODEL_URN (x -> a) is cleared. The edge to the other logical
    // parent (z -> a) must never be counted or torn down as part of this parent's teardown: its
    // mapped field-path coincidentally collides with the affected path "a", so if the cross-parent
    // isolation filter in SchemaMetadataUtils.childColumnMappings regressed and let it leak in
    // as though it mapped to MODEL_URN, "z" would be wrongly cleared too.
    assertTrue(clearsColumnEdge(sent, childUrn, "x"), "expected a->x edge cleared");
    assertFalse(clearsColumnEdge(sent, childUrn, "y"), "expected b->y edge preserved");
    assertFalse(
        clearsColumnEdge(sent, childUrn, "z"), "expected edge to other logical parent preserved");
    assertFalse(clearsDatasetLink(sent, childUrn), "expected dataset-level link preserved");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBreakingEditClearsDatasetLinkWhenChildLosesAllMappings() throws Exception {
    // Parent has only column "a"; the edit removes it and adds "c".
    SchemaMetadata existing =
        SchemaMetadataUtils.buildSchemaMetadata(
            "s", PLATFORM, List.of(col("a", SchemaFieldDataType.NUMBER)));
    Urn childUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)");

    EntityClient client = mock(EntityClient.class);
    GraphClient graph = mock(GraphClient.class);
    when(client.getV2(any(), anyString(), eq(UrnUtils.getUrn(MODEL_URN)), anySet()))
        .thenReturn(responseWith(existing));
    // Child maps only {a->x}. Dropping "a" leaves it with zero surviving mappings.
    stubChildMappings(client, Map.of(childUrn, Map.of("x", "a")));
    when(graph.getRelatedEntities(anyString(), anySet(), any(), anyInt(), anyInt(), anyString()))
        .thenReturn(page(List.of(childUrn), 0, 1));

    UpdateLogicalModelSchemaResolver resolver = new UpdateLogicalModelSchemaResolver(client, graph);
    resolver
        .get(env(getMockAllowContext(), input(List.of(col("c", SchemaFieldDataType.NUMBER)))))
        .get();

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(client, times(1)).batchIngestProposals(any(), captor.capture(), eq(false));
    List<MetadataChangeProposal> sent = captor.getValue();

    assertTrue(clearsColumnEdge(sent, childUrn, "x"), "expected a->x edge cleared");
    assertTrue(
        clearsDatasetLink(sent, childUrn),
        "expected dataset-level link cleared when no mapping survives");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testPaginationTerminatesOnUnderFullPageNotStaleTotal() throws Exception {
    SchemaMetadata existing =
        SchemaMetadataUtils.buildSchemaMetadata(
            "s", PLATFORM, List.of(col("a", SchemaFieldDataType.NUMBER)));
    Urn childUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t1,PROD)");

    EntityClient client = mock(EntityClient.class);
    GraphClient graph = mock(GraphClient.class);
    when(client.getV2(any(), anyString(), eq(UrnUtils.getUrn(MODEL_URN)), anySet()))
        .thenReturn(responseWith(existing));
    stubChildMappings(client, Map.of(childUrn, Map.of("x", "a")));

    // A single under-full page (size 1 < CHILD_PAGE_SIZE) whose reported total (99) is stale and
    // does not match reality. The stale total must NOT drive termination — the under-full page
    // size does. A second call is stubbed to return an empty page so that if the loop wrongly
    // continued, the test would still complete but the call count assertion would catch it.
    when(graph.getRelatedEntities(anyString(), anySet(), any(), anyInt(), anyInt(), anyString()))
        .thenReturn(page(List.of(childUrn), 0, 99), page(List.of(), 1, 99));

    UpdateLogicalModelSchemaResolver resolver = new UpdateLogicalModelSchemaResolver(client, graph);
    resolver
        .get(env(getMockAllowContext(), input(List.of(col("c", SchemaFieldDataType.NUMBER)))))
        .get();

    // Terminated after exactly one fetch: the under-full page size governs, not the stale total.
    verify(graph, times(1))
        .getRelatedEntities(anyString(), anySet(), any(), anyInt(), anyInt(), anyString());
  }

  @Test
  public void testUnauthorizedThrows() throws Exception {
    EntityClient client = mock(EntityClient.class);
    GraphClient graph = mock(GraphClient.class);
    UpdateLogicalModelSchemaResolver resolver = new UpdateLogicalModelSchemaResolver(client, graph);
    assertThrows(
        CompletionException.class,
        () ->
            resolver
                .get(
                    env(
                        getMockDenyContextWithOperationContext(),
                        input(List.of(col("id", SchemaFieldDataType.NUMBER)))))
                .join());
  }

  @Test
  public void testEmptyColumnsThrows() throws Exception {
    EntityClient client = mock(EntityClient.class);
    GraphClient graph = mock(GraphClient.class);
    UpdateLogicalModelSchemaResolver resolver = new UpdateLogicalModelSchemaResolver(client, graph);
    assertThrows(
        CompletionException.class,
        () -> resolver.get(env(getMockAllowContext(), input(List.of()))).join());
  }
}
