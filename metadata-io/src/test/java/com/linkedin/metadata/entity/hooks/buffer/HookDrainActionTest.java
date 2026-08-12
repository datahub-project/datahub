package com.linkedin.metadata.entity.hooks.buffer;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.buffer.offload.OffloadBuffer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavior tests for {@link HookDrainAction} — the hook-specific replay logic: group replay,
 * hook-not-found drop, group-generation-failure per-MCL isolation, per-MCL poison after {@link
 * HookPayload#isPoison()}, and emit-failure leaves-entries-for-retry. Uses an in-memory fake buffer
 * (no Hazelcast) and a real {@link TestEntityRegistry} so {@link
 * com.linkedin.metadata.entity.ebean.batch.MCLItemImpl} can build a real dataset/status MCL.
 */
public class HookDrainActionTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
  private static final String HOOK_ID =
      "com.linkedin.metadata.entity.hooks.buffer.HookDrainActionTest$TestHook";

  private EntityRegistry registry;
  private OperationContext opContext;
  private FakeBuffer buffer;
  private CapturingSink sink;
  private TestHook hook;

  @BeforeMethod
  public void setup() {
    // Spy a real registry so MCLItemImpl.build resolves real dataset/status specs, while
    // getAllMCPSideEffects returns our test hook (the test registry yml registers none).
    registry = Mockito.spy(new TestEntityRegistry());
    hook = new TestHook();
    Mockito.doReturn(List.of(hook)).when(registry).getAllMCPSideEffects();
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(registry);
    buffer = new FakeBuffer();
    sink = new CapturingSink();
  }

  /** (a) Successful group replay: hook generates, sink emits, all entries removed. */
  @Test
  public void testGroupReplayRemovesEntries() {
    hook.output = List.of(Mockito.mock(MCPItem.class));
    Map.Entry<HookKey, HookPayload> e1 = enqueue(1);
    Map.Entry<HookKey, HookPayload> e2 = enqueue(2);

    new HookDrainAction(sink, null).apply(List.of(e1, e2), opContext, buffer);

    assertEquals(sink.emitCalls, 1);
    assertEquals(sink.lastEmittedSize, 1);
    assertEquals(buffer.removes.size(), 2);
    assertTrue(buffer.pending.isEmpty());
  }

  /** (b) Hook not found in registry: entries dropped (removed), no emit. */
  @Test
  public void testHookNotFoundDropsEntries() {
    Mockito.doReturn(List.of()).when(registry).getAllMCPSideEffects();
    Map.Entry<HookKey, HookPayload> e1 = enqueue(1);
    Map.Entry<HookKey, HookPayload> e2 = enqueue(2);

    new HookDrainAction(sink, null).apply(List.of(e1, e2), opContext, buffer);

    assertEquals(sink.emitCalls, 0);
    assertEquals(buffer.removes.size(), 2);
    assertTrue(buffer.pending.isEmpty());
  }

  /**
   * (c) Group generation failure isolates per-MCL: the group call throws, so the action replays
   * each entry singly. The hook throws for a multi-item group but succeeds for a single item, so
   * each per-MCL replay succeeds and is removed.
   */
  @Test
  public void testGroupGenerationFailureIsolatesPerMcl() {
    hook.throwOnGroup = true;
    hook.output = List.of(Mockito.mock(MCPItem.class));
    Map.Entry<HookKey, HookPayload> e1 = enqueue(1);
    Map.Entry<HookKey, HookPayload> e2 = enqueue(2);

    new HookDrainAction(sink, null).apply(List.of(e1, e2), opContext, buffer);

    assertEquals(sink.emitCalls, 2);
    assertEquals(buffer.removes.size(), 2);
    assertEquals(buffer.requeues.size(), 0);
    assertTrue(buffer.pending.isEmpty());
  }

  /**
   * (d) Per-MCL poison after MAX_RETRIES: a single-entry group whose replay fails and whose payload
   * is already at the retry cap is dropped (removed, not requeued).
   */
  @Test
  public void testPerMclPoisonDroppedAfterMaxRetries() {
    hook.throwOnSingle = true;
    Map.Entry<HookKey, HookPayload> poison = enqueueAtRetry(1, HookPayload.MAX_RETRIES - 1);

    new HookDrainAction(sink, null).apply(List.of(poison), opContext, buffer);

    assertEquals(sink.emitCalls, 0);
    assertEquals(buffer.removes.size(), 1);
    assertEquals(buffer.requeues.size(), 0);
    assertTrue(buffer.pending.isEmpty());
  }

  /** A single-entry group whose replay fails but is not yet poison is requeued (retry). */
  @Test
  public void testSingleReplayFailureRequeues() {
    hook.throwOnSingle = true;
    Map.Entry<HookKey, HookPayload> e = enqueue(1);

    new HookDrainAction(sink, null).apply(List.of(e), opContext, buffer);

    assertEquals(sink.emitCalls, 0);
    assertEquals(buffer.removes.size(), 1);
    assertEquals(buffer.requeues.size(), 1);
    assertFalse(buffer.pending.isEmpty());
    HookPayload requeued = buffer.pending.values().iterator().next();
    assertEquals(requeued.getRetryCount(), 1);
  }

  /**
   * (e) Emit failure leaves entries for retry: generation succeeds, sink.emit throws, entries are
   * NOT removed (at-least-once).
   */
  @Test
  public void testEmitFailureLeavesEntriesForRetry() {
    hook.output = List.of(Mockito.mock(MCPItem.class));
    sink.throwOnEmit = true;
    Map.Entry<HookKey, HookPayload> e1 = enqueue(1);
    Map.Entry<HookKey, HookPayload> e2 = enqueue(2);

    new HookDrainAction(sink, null).apply(List.of(e1, e2), opContext, buffer);

    assertEquals(sink.emitCalls, 1);
    assertEquals(buffer.removes.size(), 0);
    assertEquals(buffer.requeues.size(), 0);
    assertEquals(buffer.pending.size(), 2);
  }

  // ---- helpers ----

  private Map.Entry<HookKey, HookPayload> enqueue(long seq) {
    return enqueueAtRetry(seq, 0);
  }

  private Map.Entry<HookKey, HookPayload> enqueueAtRetry(long seq, int retryCount) {
    HookKey key = new SimpleHookKey(HOOK_ID, DATASET_URN.toString(), STATUS_ASPECT_NAME, seq);
    HookPayload payload = statusPayload(retryCount);
    buffer.pending.put(key, payload);
    return Map.entry(key, payload);
  }

  private static HookPayload statusPayload(int retryCount) {
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(DATASET_URN);
    mcl.setEntityType(DATASET_URN.getEntityType());
    mcl.setAspectName(STATUS_ASPECT_NAME);
    mcl.setChangeType(ChangeType.UPSERT);
    mcl.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    return new HookPayload(RecordUtils.toJsonString(mcl), retryCount);
  }

  private static AspectPluginConfig hookConfig(String className) {
    return AspectPluginConfig.builder()
        .className(className)
        .enabled(true)
        .supportedOperations(List.of(ChangeType.UPSERT.toString()))
        .supportedEntityAspectNames(
            List.of(
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName(DATASET_URN.getEntityType())
                    .aspectName(STATUS_ASPECT_NAME)
                    .build()))
        .build();
  }

  /** A controllable hook: throws on the group path or the single path on demand. */
  static final class TestHook extends MCPSideEffect {
    List<MCPItem> output = List.of();
    boolean throwOnGroup = false;
    boolean throwOnSingle = false;
    private AspectPluginConfig config = hookConfig(HOOK_ID);

    @Override
    protected Stream<ChangeMCP> applyMCPSideEffect(
        @Nonnull OperationFingerprint operationFingerprint,
        @Nonnull Collection<ChangeMCP> changeMCPS,
        @Nonnull RetrieverContext retrieverContext) {
      return Stream.empty();
    }

    @Override
    protected Stream<MCPItem> postMCPSideEffect(
        @Nonnull OperationFingerprint operationFingerprint,
        @Nonnull Collection<MCLItem> mclItems,
        @Nonnull RetrieverContext retrieverContext) {
      if (mclItems.size() > 1) {
        if (throwOnGroup) {
          throw new RuntimeException("group-gen-fail");
        }
      } else if (throwOnSingle) {
        throw new RuntimeException("single-gen-fail");
      }
      // Return a fresh stream each call — a Stream is single-use and replaySingle may invoke this
      // multiple times for one apply() (group path then per-MCL isolation).
      return output.stream();
    }

    @Override
    public AspectPluginConfig getConfig() {
      return config;
    }

    @Override
    public MCPSideEffect setConfig(@Nonnull AspectPluginConfig config) {
      this.config = config;
      return this;
    }
  }

  /** Sink that counts emits and can throw on demand. */
  static final class CapturingSink implements PostCommitHookSink {
    int emitCalls = 0;
    int lastEmittedSize = -1;
    boolean throwOnEmit = false;

    @Override
    public void emit(@Nonnull OperationContext opContext, @Nonnull List<MCPItem> mcps) {
      emitCalls++;
      lastEmittedSize = mcps.size();
      if (throwOnEmit) {
        throw new RuntimeException("emit-fail");
      }
    }
  }

  /** In-memory {@link OffloadBuffer} recording removeIfSame/requeue for assertions. */
  static final class FakeBuffer implements OffloadBuffer<HookKey, HookPayload> {
    final LinkedHashMap<HookKey, HookPayload> pending = new LinkedHashMap<>();
    final List<HookKey> removes = new ArrayList<>();
    final List<HookKey> requeues = new ArrayList<>();

    @Override
    public boolean enqueue(@Nonnull HookKey key, @Nonnull HookPayload value) {
      pending.put(key, value);
      return true;
    }

    @Override
    public long nextSequence() {
      return pending.size() + 1;
    }

    @Override
    @Nonnull
    public List<Map.Entry<HookKey, HookPayload>> drain(int limit) {
      List<Map.Entry<HookKey, HookPayload>> out = new ArrayList<>();
      int n = 0;
      for (Map.Entry<HookKey, HookPayload> e : pending.entrySet()) {
        if (n++ >= limit) {
          break;
        }
        out.add(e);
      }
      return out;
    }

    @Override
    public boolean removeIfSame(@Nonnull HookKey key, @Nonnull HookPayload expected) {
      removes.add(key);
      return pending.remove(key, expected);
    }

    @Override
    public void requeue(@Nonnull HookKey key, @Nonnull HookPayload value) {
      requeues.add(key);
      pending.put(key, value);
    }

    @Override
    public boolean defersApply() {
      return true;
    }

    @Override
    public int size() {
      return pending.size();
    }

    @Nullable
    @Override
    public Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
      return "token";
    }

    @Override
    public void releaseDrainLock(@Nonnull String lockName, @Nonnull Object token) {}
  }
}
