package com.linkedin.metadata.search.semantic.profile;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.common.SemanticWorkState;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.aspect.patch.template.common.SemanticContentTemplate;
import com.linkedin.metadata.aspect.patch.template.common.SemanticProfileConflictException;
import com.linkedin.metadata.search.semantic.profile.SemanticContentApplyService.Removal;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileEligibility.Excluded;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

public class SemanticContentApplyServiceTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:document:one");

  private static final SemanticProfileRegistry REGISTRY =
      new SemanticProfileRegistry() {
        @Override
        public boolean generationEnabled() {
          return true;
        }

        @Override
        public boolean isServiceProfile(@Nonnull String entityType, @Nonnull String profileKey) {
          return true;
        }

        @Nonnull
        @Override
        public ContentBinding bindingFor(@Nonnull String entityType, @Nonnull String profileKey) {
          return new ContentBinding(Map.of("documentInfo", Set.of()));
        }
      };

  @Test
  public void testDropsWhenTheEntityIsMissing() {
    FakeStore store = new FakeStore();
    store.exists = false;

    assertEquals(
        service(store).applyResult(null, URN, "a", profile(), marker(1), "hash"),
        ApplyOutcome.DROPPED_ENTITY_MISSING);
  }

  @Test
  public void testDropsWhenTheHashDoesNotMatch() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "stored-hash");

    assertEquals(
        service(store).applyResult(null, URN, "a", profile(), marker(1), "other-hash"),
        ApplyOutcome.DROPPED_HASH_MISMATCH);
  }

  @Test
  public void testDropsAStaleResult() {
    FakeStore store = new FakeStore();
    SemanticContent content = content("a", "hash");
    content.getEmbeddings().get("a").setRefreshMarker(marker(5));
    store.stored = content;

    assertEquals(
        service(store).applyResult(null, URN, "a", profile(), marker(4), "hash"),
        ApplyOutcome.DROPPED_STALE_RESULT);
  }

  @Test
  public void testDropsWhenIneligible() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");
    SemanticContentApplyService service =
        new SemanticContentApplyService(
            store, (context, urn, key, proposed) -> new Excluded("no", "p1"), REGISTRY);

    assertEquals(
        service.applyResult(null, URN, "a", profile(), marker(1), "hash"),
        ApplyOutcome.DROPPED_INELIGIBLE);
  }

  @Test
  public void testASiblingWriteDoesNotConflict() {
    FakeStore store = new FakeStore();
    SemanticContent both = content("a", "hash-a");
    both.getEmbeddings().put("b", storedProfile("hash-b"));
    store.stored = both;
    store.beforePatch = stored -> stored.getEmbeddings().get("a").setContentHash("changed-by-a");

    assertEquals(
        service(store).applyResult(null, URN, "b", profile(), marker(1), "hash-b"),
        ApplyOutcome.ACCEPTED);
    assertEquals(store.attempts, 1);
    assertEquals(store.stored.getEmbeddings().get("a").getContentHash(), "changed-by-a");
    assertEquals(store.stored.getEmbeddings().get("b").getVectorState(), SemanticVectorState.READY);
  }

  @Test
  public void testAConflictOnTheSameKeyIsRetriedFromAFreshRead() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");
    store.beforePatch =
        stored -> {
          if (store.attempts == 1) {
            stored.getEmbeddings().get("a").setGeneratedAt(99L);
          }
        };

    assertEquals(
        service(store).applyResult(null, URN, "a", profile(), marker(1), "hash"),
        ApplyOutcome.ACCEPTED);
    assertEquals(store.attempts, 2);
    assertEquals(store.stored.getEmbeddings().get("a").getVectorState(), SemanticVectorState.READY);
  }

  @Test
  public void testAContentEditBetweenReadAndWriteIsRetried() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");
    store.beforePatch =
        stored -> {
          if (store.attempts == 1) {
            stored.setRefreshMarker(marker(7));
          }
        };

    assertEquals(
        service(store).applyResult(null, URN, "a", profile(), marker(1), "hash"),
        ApplyOutcome.ACCEPTED);
    assertEquals(store.attempts, 2);
    assertFalse(store.stored.getEmbeddings().get("a").isCurrent());
  }

  @Test
  public void testAResultOlderThanABoundSourceIsStoredAsNotCurrent() {
    FakeStore store = new FakeStore();
    SemanticContent content = content("a", "hash");
    content.setRefreshMarker(marker(10));
    store.stored = content;

    assertEquals(
        service(store).applyResult(null, URN, "a", profile(), marker(5), "hash"),
        ApplyOutcome.ACCEPTED);
    EmbeddingModelData written = store.stored.getEmbeddings().get("a");
    assertEquals(written.getVectorState(), SemanticVectorState.READY);
    assertFalse(written.isCurrent());
  }

  @Test
  public void testAProfileSharingTheAspectClockIsNotCurrentOnTheNextApply() {
    FakeStore store = new FakeStore();
    SemanticContent content = content("other", "hash");
    content.getEmbeddings().get("other").setCurrent(true);
    content.setRefreshMarker(marker(80));
    store.stored = content;

    assertEquals(
        service(store).applyResult(null, URN, "other", profile(), marker(10), "hash"),
        ApplyOutcome.ACCEPTED);
    assertFalse(store.stored.getEmbeddings().get("other").isCurrent());
  }

  @Test
  public void testAResultCoveringTheBoundSourcesIsCurrent() {
    FakeStore store = new FakeStore();
    SemanticContent content = content("a", "hash");
    content.setRefreshMarker(marker(10));
    store.stored = content;

    assertEquals(
        service(store).applyResult(null, URN, "a", profile(), marker(10), "hash"),
        ApplyOutcome.ACCEPTED);
    assertTrue(store.stored.getEmbeddings().get("a").isCurrent());
  }

  @Test
  public void testAClockOnAnotherPathOfTheSameAspectDoesNotStaleAResult() {
    FakeStore store = new FakeStore();
    SemanticContent content = content("body", "hash");
    LongMap titleClock = new LongMap();
    titleClock.put("documentInfo#title", 80L);
    content.setRefreshMarker(new SemanticRefreshMarker().setSources(titleClock));
    store.stored = content;
    SemanticProfileRegistry bodyOnly =
        new SemanticProfileRegistry() {
          @Override
          public boolean generationEnabled() {
            return true;
          }

          @Override
          public boolean isServiceProfile(@Nonnull String entityType, @Nonnull String profileKey) {
            return true;
          }

          @Nonnull
          @Override
          public ContentBinding bindingFor(@Nonnull String entityType, @Nonnull String profileKey) {
            return new ContentBinding(Map.of("documentInfo", Set.of("contents.text")));
          }
        };
    LongMap representedClock = new LongMap();
    representedClock.put("documentInfo#title", 10L);

    assertEquals(
        new SemanticContentApplyService(store, new AlwaysEligible(), bodyOnly)
            .applyResult(
                null,
                URN,
                "body",
                profile(),
                new SemanticRefreshMarker().setSources(representedClock),
                "hash"),
        ApplyOutcome.ACCEPTED);
    EmbeddingModelData written = store.stored.getEmbeddings().get("body");
    assertTrue(written.isCurrent());
    assertFalse(written.getRefreshMarker().getSources().containsKey("documentInfo#title"));
  }

  @Test
  public void testStateOnlyKeepsVectorsStoredByAConcurrentResult() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");
    store.beforePatch =
        stored -> stored.getEmbeddings().get("a").setVectorState(SemanticVectorState.READY);

    assertEquals(
        service(store)
            .applyStateOnly(
                null, URN, "a", "model", SemanticWorkState.PENDING, "queued", "hash", null),
        ApplyOutcome.ACCEPTED);
    EmbeddingModelData written = store.stored.getEmbeddings().get("a");
    assertEquals(written.getWorkState(), SemanticWorkState.PENDING);
    assertEquals(written.getStateReason(), "queued");
    assertEquals(written.getVectorState(), SemanticVectorState.READY);
  }

  @Test
  public void testStateOnlyDropsWhenTheHashMoved() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");
    store.beforePatch = stored -> stored.getEmbeddings().get("a").setContentHash("newer");

    assertEquals(
        service(store)
            .applyStateOnly(
                null, URN, "a", "model", SemanticWorkState.PENDING, "queued", "hash", null),
        ApplyOutcome.DROPPED_STALE_WORK);
  }

  @Test
  public void testRemovalDropsAMissingEntity() {
    FakeStore store = new FakeStore();
    store.exists = false;

    assertEquals(
        service(store).applyRemoval(null, URN, "a", "model", Removal.SKIP, "empty"),
        ApplyOutcome.DROPPED_ENTITY_MISSING);
  }

  @Test
  public void testAMissingMarkerEntryDoesNotMakeTheResultStale() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");

    assertEquals(
        service(store).applyResult(null, URN, "a", profile(), marker(5), "hash"),
        ApplyOutcome.ACCEPTED);
    assertTrue(store.stored.getEmbeddings().get("a").isCurrent());
  }

  @Test
  public void testStateOnlyDropsWhenGeneratedAtChangesBeforeTheRetry() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");
    store.stored.getEmbeddings().get("a").setWorkState(SemanticWorkState.NONE);
    store.beforePatch =
        stored -> {
          if (store.attempts == 1) {
            stored.getEmbeddings().get("a").setGeneratedAt(99L);
          }
        };

    assertEquals(
        service(store)
            .applyStateOnly(null, URN, "a", "model", SemanticWorkState.PENDING, null, "hash", null),
        ApplyOutcome.DROPPED_STALE_WORK);
    assertEquals(store.stored.getEmbeddings().get("a").getWorkState(), SemanticWorkState.NONE);
    assertEquals(store.stored.getEmbeddings().get("a").getGeneratedAt(), Long.valueOf(99L));
    assertEquals(store.attempts, 1);
  }

  @Test
  public void testStateOnlyDropsWhenWorkStateChangesBeforeTheRetry() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");
    store.stored.getEmbeddings().get("a").setWorkState(SemanticWorkState.NONE);
    store.beforePatch =
        stored -> {
          if (store.attempts == 1) {
            stored.getEmbeddings().get("a").setWorkState(SemanticWorkState.QUARANTINED);
          }
        };

    assertEquals(
        service(store)
            .applyStateOnly(null, URN, "a", "model", SemanticWorkState.PENDING, null, "hash", null),
        ApplyOutcome.DROPPED_STALE_WORK);
    assertEquals(
        store.stored.getEmbeddings().get("a").getWorkState(), SemanticWorkState.QUARANTINED);
    assertEquals(store.attempts, 1);
  }

  @Test
  public void testRemovalClearsResultFields() {
    FakeStore store = new FakeStore();
    SemanticContent content = content("a", "hash");
    content
        .getEmbeddings()
        .get("a")
        .setCurrent(true)
        .setRefreshMarker(marker(3))
        .setWorkState(SemanticWorkState.PENDING);
    store.stored = content;

    assertEquals(
        service(store).applyRemoval(null, URN, "a", "model", Removal.EXCLUDE, "policy"),
        ApplyOutcome.ACCEPTED);
    EmbeddingModelData written = store.stored.getEmbeddings().get("a");
    assertEquals(written.getVectorState(), SemanticVectorState.EXCLUDED);
    assertEquals(written.getWorkState(), SemanticWorkState.NONE);
    assertNull(written.isCurrent());
    assertNull(written.getContentHash());
    assertNull(written.getRefreshMarker());
    assertEquals(written.getTotalChunks(), Integer.valueOf(0));
  }

  @Test
  public void testRemovalWithoutAReasonIsRejected() {
    FakeStore store = new FakeStore();
    store.stored = content("a", "hash");

    assertThrows(
        IllegalArgumentException.class,
        () -> service(store).applyRemoval(null, URN, "a", "model", Removal.SKIP, " "));
    assertEquals(store.attempts, 0);
  }

  @Test
  public void testRemovingAnAbsentKeyDoesNotWrite() {
    FakeStore store = new FakeStore();
    store.stored = content("b", "hash-b");

    assertEquals(
        service(store).applyRemoval(null, URN, "a", "model", Removal.REMOVE_KEY, null),
        ApplyOutcome.ACCEPTED);
    assertEquals(store.attempts, 0);
    assertFalse(store.stored.getEmbeddings().containsKey("a"));
  }

  @Test
  public void testRemovalOfAMissingKeyWritesACompleteProfile() {
    FakeStore store = new FakeStore();
    store.stored = content("b", "hash-b");

    assertEquals(
        service(store).applyRemoval(null, URN, "a", "embed-model", Removal.SKIP, "EMPTY_TEXT"),
        ApplyOutcome.ACCEPTED);
    EmbeddingModelData written = store.stored.getEmbeddings().get("a");
    assertEquals(written.getModelVersion(), "embed-model");
    assertEquals(written.getVectorState(), SemanticVectorState.SKIPPED);
    assertEquals(written.getStateReason(), "EMPTY_TEXT");
    assertEquals(store.stored.getEmbeddings().get("b").getContentHash(), "hash-b");
  }

  @Test
  public void testStateOnlyOfAMissingKeyWritesACompleteProfile() {
    FakeStore store = new FakeStore();
    store.stored = content("b", "hash-b");

    assertEquals(
        service(store)
            .applyStateOnly(
                null, URN, "a", "embed-model", SemanticWorkState.PENDING, "queued", null, "policy"),
        ApplyOutcome.ACCEPTED);
    EmbeddingModelData written = store.stored.getEmbeddings().get("a");
    assertNotNull(written);
    assertEquals(written.getModelVersion(), "embed-model");
    assertEquals(written.getWorkState(), SemanticWorkState.PENDING);
    assertEquals(written.getPolicyVersion(), "policy");
  }

  private static SemanticContentApplyService service(FakeStore store) {
    return new SemanticContentApplyService(store, new AlwaysEligible(), REGISTRY);
  }

  private static EmbeddingModelData profile() {
    return new EmbeddingModelData()
        .setModelVersion("model")
        .setGeneratedAt(1L)
        .setTotalChunks(0)
        .setChunks(new EmbeddingChunkArray())
        .setContentHash("hash");
  }

  private static EmbeddingModelData storedProfile(String hash) {
    return profile().setContentHash(hash);
  }

  private static SemanticContent content(String key, String hash) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put(key, storedProfile(hash));
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static SemanticRefreshMarker marker(long time) {
    LongMap sources = new LongMap();
    sources.put("documentInfo", time);
    return new SemanticRefreshMarker().setSources(sources);
  }

  /** Applies patches through {@link SemanticContentTemplate}, so preconditions are evaluated. */
  private static final class FakeStore implements SemanticContentStore {
    private static final SemanticContentTemplate TEMPLATE = new SemanticContentTemplate();

    private boolean exists = true;
    private SemanticContent stored;
    private int attempts;
    private Consumer<SemanticContent> beforePatch = stored -> {};
    private final List<MetadataChangeProposal> patches = new ArrayList<>();

    @Override
    public boolean exists(@Nonnull OperationContext context, @Nonnull Urn urn) {
      return exists;
    }

    @Nullable
    @Override
    public SemanticContent get(@Nonnull OperationContext context, @Nonnull Urn urn) {
      try {
        return stored == null ? null : stored.copy();
      } catch (CloneNotSupportedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public boolean patch(
        @Nonnull OperationContext context,
        @Nonnull Urn urn,
        @Nonnull MetadataChangeProposal patch) {
      attempts++;
      if (stored == null) {
        stored = TEMPLATE.getDefault();
      }
      beforePatch.accept(stored);
      String body = new String(patch.getAspect().getValue().copyBytes(), StandardCharsets.UTF_8);
      JsonPatch jsonPatch = Json.createPatch(Json.createReader(new StringReader(body)).readArray());
      try {
        stored = TEMPLATE.applyPatch(stored, jsonPatch);
      } catch (SemanticProfileConflictException e) {
        return false;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      patches.add(patch);
      return true;
    }
  }
}
