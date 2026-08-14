package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ParentNodesResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.Constants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dataloader.Try;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParentNodesBatchLoaderTest {

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:glossaryNode:root");
  private static final Urn MID = UrnUtils.getUrn("urn:li:glossaryNode:mid");
  private static final Urn TERM_A = UrnUtils.getUrn("urn:li:glossaryTerm:a");
  private static final Urn TERM_B = UrnUtils.getUrn("urn:li:glossaryTerm:b");

  private EntityClient _entityClient;
  private QueryContext _context;

  @BeforeMethod
  public void setup() {
    _entityClient = Mockito.mock(EntityClient.class);
    _context = getMockAllowContext();
  }

  private static EntityResponse nodeResponse(Urn urn) {
    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(
                new Aspect(new GlossaryNodeInfo().setName(urn.getId()).setDefinition("d").data())));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(Constants.GLOSSARY_NODE_ENTITY_NAME)
        .setAspects(new EnvelopedAspectMap(aspects));
  }

  @SuppressWarnings("unchecked")
  private void stubBatchGet(Set<Urn> known) throws Exception {
    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenAnswer(
            inv -> {
              final Set<Urn> requested = inv.getArgument(2);
              final Map<Urn, EntityResponse> out = new HashMap<>();
              for (Urn u : requested) {
                if (known.contains(u)) {
                  out.put(u, nodeResponse(u));
                }
              }
              return out;
            });
  }

  @SuppressWarnings("unchecked")
  private ArgumentCaptor<Set<Urn>> captureBatchGet(int times) throws Exception {
    final ArgumentCaptor<Set<Urn>> captor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(_entityClient, Mockito.times(times))
        .batchGetV2(any(), any(String.class), captor.capture(), nullable(Set.class));
    return captor;
  }

  /** Two terms under the same node must cost one hydration call, not two. */
  @Test
  public void testSharedAncestorsFetchedOnce() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains =
        Map.of(TERM_A, List.of(MID, ROOT), TERM_B, List.of(MID, ROOT));

    final List<Try<ParentNodesResult>> out =
        ParentNodesBatchLoader.batchLoadForTest(
            List.of(TERM_A, TERM_B), _context, _entityClient, chains);

    assertEquals(out.get(0).get().getCount(), 2);
    assertEquals(out.get(1).get().getCount(), 2);
    assertEquals(captureBatchGet(1).getValue(), Set.of(MID, ROOT));
  }

  /** Hierarchy order must survive the shared, unordered response map. */
  @Test
  public void testHierarchyOrderPreserved() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));

    final ParentNodesResult result =
        ParentNodesBatchLoader.batchLoadForTest(
                List.of(TERM_A), _context, _entityClient, Map.of(TERM_A, List.of(MID, ROOT)))
            .get(0)
            .get();

    assertEquals(result.getNodes().get(0).getUrn(), MID.toString());
    assertEquals(result.getNodes().get(1).getUrn(), ROOT.toString());
  }

  @Test
  public void testNoAncestorsIssuesNoFetch() throws Exception {
    stubBatchGet(Set.of());

    final List<Try<ParentNodesResult>> out =
        ParentNodesBatchLoader.batchLoadForTest(
            List.of(TERM_A), _context, _entityClient, Map.of(TERM_A, List.of()));

    assertEquals(out.get(0).get().getCount(), 0);
    Mockito.verify(_entityClient, Mockito.never())
        .batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class));
  }

  /**
   * An unresolvable ancestor fails only the entity that referenced it. The unbatched resolver threw
   * for that one field, so sharing a batch must not spread the failure.
   */
  @Test
  public void testMissingAncestorFailsOnlyItsOwnKey() throws Exception {
    stubBatchGet(Set.of(ROOT)); // MID unresolvable
    final Map<Urn, List<Urn>> chains = Map.of(TERM_A, List.of(MID, ROOT), TERM_B, List.of(ROOT));

    final List<Try<ParentNodesResult>> out =
        ParentNodesBatchLoader.batchLoadForTest(
            List.of(TERM_A, TERM_B), _context, _entityClient, chains);

    assertTrue(out.get(0).isFailure(), "term with the missing ancestor should fail");
    assertTrue(out.get(1).isSuccess(), "the other term must still resolve");
    assertEquals(out.get(1).get().getCount(), 1);
  }

  /** A failed hydration is shared by the whole batch, so every key sees it. */
  @Test
  public void testFetchFailurePropagatesToAllKeys() throws Exception {
    Mockito.when(
            _entityClient.batchGetV2(any(), any(String.class), any(Set.class), nullable(Set.class)))
        .thenThrow(new RuntimeException("entity client unavailable"));

    final List<Try<ParentNodesResult>> out =
        ParentNodesBatchLoader.batchLoadForTest(
            List.of(TERM_A, TERM_B),
            _context,
            _entityClient,
            Map.of(TERM_A, List.of(ROOT), TERM_B, List.of(ROOT)));

    assertTrue(out.get(0).isFailure());
    assertTrue(out.get(1).isFailure());
  }

  /** DataLoader's contract is positional, including when a key repeats in one batch. */
  @Test
  public void testDuplicateKeysMapBackPositionally() throws Exception {
    stubBatchGet(Set.of(ROOT, MID));
    final Map<Urn, List<Urn>> chains = Map.of(TERM_A, List.of(MID, ROOT), TERM_B, List.of(ROOT));

    final List<Try<ParentNodesResult>> out =
        ParentNodesBatchLoader.batchLoadForTest(
            List.of(TERM_A, TERM_B, TERM_A), _context, _entityClient, chains);

    assertEquals(out.size(), 3);
    assertEquals(out.get(0).get().getCount(), 2);
    assertEquals(out.get(1).get().getCount(), 1);
    assertEquals(out.get(2).get().getCount(), 2);
  }
}
