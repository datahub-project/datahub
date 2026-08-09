/**
 * Offset-checkpoint "wait for writes to sync" — TypeScript port of the pytest
 * implementation in smoke-test/tests/consistency_utils.py
 * (wait_for_offsets_to_be_consumed, introduced in #18881).
 *
 * DataHub writes flow MCP → (mce-consumer) → SQL + MCL → (mae-consumer) →
 * search index. A test that writes and then reads through a search-backed
 * surface must wait for that pipeline; a fixed sleep is either too long
 * (quiet system) or too short (backlog). This helper instead:
 *
 *  1. Captures each topic-partition's current end-offset ONCE per phase from
 *     GET {gms}/openapi/operations/messaging/{type}/consumer/lag?detailed=true
 *     (per partition, `offset` is the consumer's committed offset and
 *     `offset + lag` is the topic end-offset). The checkpoint is fixed at
 *     call time, so later writes from concurrent workers cannot move the
 *     target — unlike polling aggregate lag to zero, which can fail to
 *     converge at all while other writers keep the lag non-zero.
 *  2. Polls (every ~250 ms) until the committed offsets pass the checkpoint.
 *     Expect ~0.3 s resolution on a quiet system.
 *
 * Two-phase MCP → MCL: on the async ingest path only the MCP exists when this
 * is called — GMS produces just the proposal and returns; the MCL is produced
 * later by the mce-consumer. So the MCP checkpoint is waited on first, and
 * only then is the MCL / MCL-timeseries checkpoint captured. Capturing both
 * up front would self-satisfy on the pre-write MCL end-offset and skip
 * indexing entirely. Synchronous writes (GraphQL mutations) ack their MCL
 * in-request, so phase 1 is a no-op for them.
 *
 * A failed checkpoint fetch (transient 401/connection error) is NOT treated
 * as "nothing to wait for" — that would return near-instantly as if the
 * system were synced. Instead we fall back to conservatively polling
 * aggregate lag to zero (1 s ticks, bounded by the deadline), and to the
 * single static ES-refresh sleep if the lag API is unreachable altogether —
 * mirroring the Python fallback chain.
 *
 * Known carve-outs (kept in parity with the Python implementation):
 *  - The usage-events consumer group is not covered by these endpoints;
 *    audit-event indexing is out of scope for this helper.
 *  - A checkpoint captured mid-burst legitimately waits for already-queued
 *    backlog from other writers. The win is eliminating the non-convergence
 *    tail risk, not genuine backlog-drain time.
 *  - CDC mode (CDC_MCL_PROCESSING_ENABLED=true, default off) produces no
 *    inline MCL, and an MCL produce failure is swallowed by the consumer;
 *    phase 2 is not a hard guarantee in those cases.
 */

import type { APIRequestContext } from '@playwright/test';
import { gmsUrl as defaultGmsUrl } from './constants';
import { readGmsToken } from '../fixtures/login';
import { users } from '../data/users';
import { createScriptLogger, type DataHubLogger } from './logger';

const LAG_ENDPOINTS = {
  mcp: '/openapi/operations/messaging/mcp/consumer/lag',
  mcl: '/openapi/operations/messaging/mcl/consumer/lag',
  'mcl-timeseries': '/openapi/operations/messaging/mcl-timeseries/consumer/lag',
} as const;

export type ConsumerType = keyof typeof LAG_ENDPOINTS;

const DEFAULT_CONSUMERS: readonly ConsumerType[] = ['mcp', 'mcl', 'mcl-timeseries'];
const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_POLL_INTERVAL_MS = 250;
const FALLBACK_POLL_INTERVAL_MS = 1_000;
const LAG_REQUEST_TIMEOUT_MS = 5_000;

export interface WritesSyncOptions {
  /** GMS base URL. Defaults to `gmsUrl()` (BASE_URL / GMS_URL aware). */
  gmsUrl?: string;
  /**
   * GMS bearer token. Defaults to the admin user's cached token from
   * `.auth/gms-token-{admin}.json` (written by the login fixture / seeding
   * bootstrap) — the same file search-data.setup.ts reads for ingestion.
   */
  gmsToken?: string;
  /** Consumers to wait on. Defaults to all three (mcp, mcl, mcl-timeseries). */
  consumers?: readonly ConsumerType[];
  /** Safety ceiling across both phases (default 60 s). */
  timeoutMs?: number;
  /** Checkpoint re-check interval (default 250 ms). */
  pollIntervalMs?: number;
  /**
   * Post-sync sleep so the newly indexed documents become visible to search.
   * Defaults to ELASTICSEARCH_REFRESH_INTERVAL_SECONDS (3 s, matching the
   * pytest default; CI quickstart configures a 1 s refresh interval).
   */
  esRefreshMarginMs?: number;
  logger?: DataHubLogger;
}

interface PartitionSnapshot {
  offset: number;
  lag: number;
}

interface LagEnvelope {
  consumerGroups?: Record<
    string,
    Record<string, { partitions?: Record<string, { offset?: number | null; lag?: number | null }> }>
  >;
}

function defaultEsRefreshMarginMs(): number {
  const seconds = Number(process.env.ELASTICSEARCH_REFRESH_INTERVAL_SECONDS ?? '3');
  return (Number.isFinite(seconds) && seconds > 0 ? seconds : 3) * 1000;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function targetKey(consumer: ConsumerType, partition: number): string {
  return `${consumer}:${partition}`;
}

function consumerOfKey(key: string): ConsumerType {
  return key.slice(0, key.lastIndexOf(':')) as ConsumerType;
}

/**
 * Fetch `{partition: {offset, lag}}` for one consumer from the detailed lag
 * endpoint. Returns null when the fetch failed or the payload is malformed
 * (missing offset), so callers can distinguish "couldn't look" from a
 * genuinely empty result.
 */
async function fetchDetailedPartitions(
  request: APIRequestContext,
  baseUrl: string,
  headers: Record<string, string>,
  consumer: ConsumerType,
): Promise<Map<number, PartitionSnapshot> | null> {
  try {
    const response = await request.get(`${baseUrl}${LAG_ENDPOINTS[consumer]}?skipCache=true&detailed=true`, {
      headers,
      timeout: LAG_REQUEST_TIMEOUT_MS,
    });
    if (!response.ok()) return null;
    const data = (await response.json()) as LagEnvelope;
    for (const topics of Object.values(data.consumerGroups ?? {})) {
      // Single consumer group per topic today; take the first group found.
      for (const topicInfo of Object.values(topics)) {
        const result = new Map<number, PartitionSnapshot>();
        for (const [partition, info] of Object.entries(topicInfo.partitions ?? {})) {
          if (info.offset === null || info.offset === undefined) return null;
          result.set(Number(partition), { offset: info.offset, lag: info.lag ?? 0 });
        }
        return result;
      }
    }
    return new Map();
  } catch {
    return null;
  }
}

/**
 * Capture the current end-offset (`offset + lag`) per (consumer, partition).
 * Returns null if any fetch failed.
 */
async function captureOffsetTargets(
  request: APIRequestContext,
  baseUrl: string,
  headers: Record<string, string>,
  consumers: readonly ConsumerType[],
): Promise<Map<string, number> | null> {
  const targets = new Map<string, number>();
  for (const consumer of consumers) {
    const partitions = await fetchDetailedPartitions(request, baseUrl, headers, consumer);
    if (partitions === null) return null;
    for (const [partition, snapshot] of partitions) {
      targets.set(targetKey(consumer, partition), snapshot.offset + snapshot.lag);
    }
  }
  return targets;
}

/**
 * Poll until every target offset has been passed, or the deadline hits.
 * Returns the number of targets still outstanding (0 means fully consumed).
 */
async function awaitOffsetTargets(
  request: APIRequestContext,
  baseUrl: string,
  headers: Record<string, string>,
  targets: Map<string, number>,
  deadline: number,
  pollIntervalMs: number,
): Promise<number> {
  const remaining = new Map(targets);
  while (remaining.size > 0 && Date.now() < deadline) {
    await delay(pollIntervalMs);
    const consumersLeft = new Set<ConsumerType>();
    for (const key of remaining.keys()) consumersLeft.add(consumerOfKey(key));
    for (const consumer of consumersLeft) {
      const partitions = await fetchDetailedPartitions(request, baseUrl, headers, consumer);
      if (partitions === null) continue;
      for (const [partition, snapshot] of partitions) {
        const key = targetKey(consumer, partition);
        const target = remaining.get(key);
        if (target !== undefined && snapshot.offset >= target) remaining.delete(key);
      }
    }
  }
  return remaining.size;
}

/**
 * Conservative fallback when a checkpoint could not be established: poll
 * aggregate lag until it reaches zero (a moving target under concurrent
 * writers, but bounded by the deadline — same trade-off as the legacy pytest
 * path). Reports 'unavailable' when the lag API cannot be reached at all.
 */
async function fallbackAggregateLagWait(
  request: APIRequestContext,
  baseUrl: string,
  headers: Record<string, string>,
  consumers: readonly ConsumerType[],
  deadline: number,
): Promise<'synced' | 'timed-out' | 'unavailable'> {
  while (Date.now() < deadline) {
    await delay(FALLBACK_POLL_INTERVAL_MS);
    let totalLag = 0;
    let apiAvailable = false;
    for (const consumer of consumers) {
      const partitions = await fetchDetailedPartitions(request, baseUrl, headers, consumer);
      if (partitions === null) continue;
      apiAvailable = true;
      for (const snapshot of partitions.values()) totalLag += snapshot.lag;
    }
    if (!apiAvailable) return 'unavailable';
    if (totalLag === 0) return 'synced';
  }
  return 'timed-out';
}

/**
 * Wait for in-flight metadata writes to be consumed and indexed, then wait
 * one ES refresh interval so they become visible to search.
 *
 * Never throws on timeout — like the pytest helper it degrades to "waited as
 * long as allowed" and lets the caller's own assertions surface the failure.
 */
export async function waitForWritesToSync(request: APIRequestContext, options: WritesSyncOptions = {}): Promise<void> {
  const logger = options.logger ?? createScriptLogger('writes-sync');
  const baseUrl = options.gmsUrl ?? defaultGmsUrl();
  const token = options.gmsToken ?? readGmsToken(users.admin.username);
  const headers = { Authorization: `Bearer ${token}` };
  const consumers = options.consumers ?? DEFAULT_CONSUMERS;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
  const esRefreshMarginMs = options.esRefreshMarginMs ?? defaultEsRefreshMarginMs();

  const start = Date.now();
  const deadline = start + timeoutMs;

  // Phase 1: MCP only; phase 2: MCL (+ timeseries), captured only after the
  // MCP checkpoint has been consumed. See the module comment for why.
  const phases: ConsumerType[][] = [consumers.filter((c) => c === 'mcp'), consumers.filter((c) => c !== 'mcp')].filter(
    (phase) => phase.length > 0,
  );

  let totalTargets = 0;
  let outstanding = 0;

  for (const phase of phases) {
    const targets = await captureOffsetTargets(request, baseUrl, headers, phase);
    if (targets === null) {
      logger.warn('Could not establish an offset checkpoint; falling back to aggregate lag polling', {
        consumers: phase.join(','),
        gmsUrl: baseUrl,
      });
      const outcome = await fallbackAggregateLagWait(request, baseUrl, headers, consumers, deadline);
      if (outcome !== 'synced') {
        logger.warn(`Fallback lag wait ended without confirming sync (${outcome}); relying on the ES refresh margin`);
      }
      await delay(esRefreshMarginMs);
      return;
    }
    totalTargets += targets.size;
    outstanding += await awaitOffsetTargets(request, baseUrl, headers, targets, deadline, pollIntervalMs);
  }

  const elapsedMs = Date.now() - start;
  if (outstanding > 0) {
    logger.warn(`Timed out after ${elapsedMs}ms waiting for ${outstanding}/${totalTargets} offset target(s)`);
  } else {
    logger.info(`All ${totalTargets} offset target(s) consumed after ${elapsedMs}ms`);
  }
  await delay(esRefreshMarginMs);
}
