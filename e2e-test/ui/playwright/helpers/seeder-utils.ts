/**
 * Shared utilities for test data seeders.
 *
 * extractUrn and waitForSync were duplicated across cli-seeder, graphql-seeder,
 * and rest-seeder. This module is the single canonical source.
 *
 * extractComplexAspects / ingestComplexAspects compensate for aspects that the
 * legacy /entities?action=ingest endpoint silently drops (Avro union types, null
 * optional fields, enum union formats). They are called from ingestMcps as a
 * second pass over the same parsed data, so no separate lineage-seeder file is
 * needed.
 */

import * as fs from 'fs';
import * as path from 'path';
import type { APIRequestContext } from '@playwright/test';
import { gmsUrl } from '../utils/constants';
import type { DataHubLogger } from '../utils/logger';

export type Urn = string;

// ── Cross-worker artifact locking ────────────────────────────────────────────

/**
 * Live holders refresh the lock mtime on this interval. Must be well below
 * LOCK_STALE_MS so a healthy producer never looks crashed to followers.
 */
const LOCK_HEARTBEAT_MS = 10_000;
/**
 * If a lock file is older than this with its artifact still missing, its
 * holder is assumed to have crashed (missed several heartbeats); another
 * worker reclaims it rather than wedging the whole run.
 */
const LOCK_STALE_MS = 45_000;
const LOCK_POLL_MS = 500;
/** How long a follower will wait for another worker's artifact before giving up. */
const LOCK_WAIT_TIMEOUT_MS = 180_000;

function newLockToken(): string {
  return `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

/** Renew mtime while we hold the lock so slow-but-live producers aren't stolen. */
function startLockHeartbeat(lockPath: string, token: string): NodeJS.Timeout {
  const heartbeat = setInterval(() => {
    try {
      if (fs.readFileSync(lockPath, 'utf-8') !== token) return;
      const now = new Date();
      fs.utimesSync(lockPath, now, now);
    } catch {
      // Lock gone or raced — produce()'s finally will no-op the release.
    }
  }, LOCK_HEARTBEAT_MS);
  heartbeat.unref();
  return heartbeat;
}

/** Remove the lock only if we still own it — never delete a reclaimer's lock. */
function releaseLockIfOwned(lockPath: string, token: string): void {
  try {
    if (fs.readFileSync(lockPath, 'utf-8') !== token) return;
    fs.rmSync(lockPath, { force: true });
  } catch {
    // Already gone.
  }
}

/**
 * Atomically claims responsibility for producing `artifactPath` across
 * concurrent worker processes sharing the same filesystem (workers_per_shard
 * > 1, or fullyParallel scheduling two files/describe-blocks that share a
 * seed routine onto different workers). `fs.existsSync` + later
 * `fs.writeFileSync` is a check-then-act race: two callers can both observe
 * "absent" before either has written, and both duplicate the work.
 * `fs.openSync(lockPath, 'wx')` is atomic exclusive create -- exactly one
 * caller can win it -- so only the winner produces the artifact while
 * everyone else polls for it instead.
 *
 * Ownership is a token written into the lock file; the holder heartbeats
 * mtime during `produce()` so LOCK_STALE_MS reclaim cannot steal a live
 * producer, and `finally` only deletes the lock when the token still matches
 * (so a finishing holder cannot clobber a reclaimer's lock).
 */
export async function withArtifactLock(artifactPath: string, produce: () => Promise<void>): Promise<void> {
  if (fs.existsSync(artifactPath)) return;

  const lockPath = `${artifactPath}.lock`;
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });
  const deadline = Date.now() + LOCK_WAIT_TIMEOUT_MS;

  for (;;) {
    if (fs.existsSync(artifactPath)) return;

    let fd: number | undefined;
    try {
      fd = fs.openSync(lockPath, 'wx');
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    }

    if (fd !== undefined) {
      const token = newLockToken();
      try {
        fs.writeFileSync(fd, token);
      } finally {
        fs.closeSync(fd);
      }
      const heartbeat = startLockHeartbeat(lockPath, token);
      try {
        if (!fs.existsSync(artifactPath)) {
          await produce();
        }
      } finally {
        clearInterval(heartbeat);
        releaseLockIfOwned(lockPath, token);
      }
      return;
    }

    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for '${artifactPath}' to be produced by another worker (lock: ${lockPath})`);
    }

    try {
      const age = Date.now() - fs.statSync(lockPath).mtimeMs;
      if (age > LOCK_STALE_MS) fs.rmSync(lockPath, { force: true });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
      // Lock file was removed between the acquisition attempt and stat/rm; re-poll.
    }

    await new Promise<void>((resolve) => setTimeout(resolve, LOCK_POLL_MS));
  }
}

/** Write JSON atomically: readers never observe a partially-written file. */
export function writeJsonAtomic(filePath: string, data: unknown): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmpPath = `${filePath}.tmp-${process.pid}`;
  fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2));
  fs.renameSync(tmpPath, filePath);
}

// ── Types ─────────────────────────────────────────────────────────────────────

export interface Mcp {
  entityUrn?: string;
  proposedSnapshot?: Record<string, { urn?: string }>;
  [key: string]: unknown;
}

export interface ComplexAspect {
  urn: string;
  entityType: string;
  aspectName: string;
  value: unknown;
}

// ── Complex-aspect extraction ─────────────────────────────────────────────────

/** Recursively remove keys whose value is null or undefined. */
export function stripNulls(obj: unknown): unknown {
  if (obj === null || obj === undefined) return undefined;
  if (Array.isArray(obj)) return obj.map(stripNulls).filter((v) => v !== undefined);
  if (typeof obj === 'object') {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      if (v !== null && v !== undefined) result[k] = stripNulls(v);
    }
    return result;
  }
  return obj;
}

/**
 * Normalise an MCP before sending to GMS:
 *  1. Strip all null/undefined fields so RestLi doesn't reject explicit nulls on optional fields.
 *  2. Convert `aspect.json: {...}` shorthand to the required GenericAspect format
 *     `aspect: { value: "...", contentType: "application/json" }`.
 */
export function normalizeMcp(mcp: Mcp): Mcp {
  const stripped = stripNulls(mcp) as Mcp;
  const aspect = (stripped as Record<string, unknown>).aspect as Record<string, unknown> | undefined;
  if (aspect && 'json' in aspect && aspect.json !== undefined) {
    (stripped as Record<string, unknown>).aspect = {
      value: JSON.stringify(aspect.json),
      contentType: 'application/json',
    };
  }

  // Strip nulls from the embedded JSON string in aspect.value.
  // Top-level null-stripping misses nulls inside the value string (e.g. auditStamp.impersonator: null).
  const normalizedAspect = (stripped as Record<string, unknown>).aspect as Record<string, unknown> | undefined;
  if (normalizedAspect && typeof normalizedAspect.value === 'string') {
    try {
      normalizedAspect.value = JSON.stringify(stripNulls(JSON.parse(normalizedAspect.value)));
    } catch {
      // not valid JSON — leave as-is
    }
  }

  return stripped;
}

/**
 * Extract aspects from parsed data.json entries that the legacy
 * /entities?action=ingest endpoint silently drops. Called with the already-parsed
 * MCP array (namespace replacement already applied by the caller).
 *
 * Transformations applied per entity type:
 *   dataset  siblings         — posted as-is (ensures merging is properly persisted)
 *   dataset  upstreamLineage  — posted as-is (the legacy endpoint fails on union types)
 *   dataset  schemaMetadata   — strip nulls; replace platformSchema with OtherSchema
 *   chart    chartInfo        — strip nulls; normalise inputs to [{"string":"urn"}]
 *   dashboard dashboardInfo   — strip nulls
 *   dataJob  dataJobInfo      — strip nulls; wrap type enum as {"string": "VALUE"}
 *   dataJob  dataJobInputOutput — strip nulls
 *   dataFlow dataFlowInfo     — strip nulls
 *   mlFeature mlFeatureProperties       — strip nulls
 *   mlPrimaryKey mlPrimaryKeyProperties — strip nulls
 *   mlModel  mlModelProperties          — strip nulls
 *   mlModelGroup mlModelGroupProperties — strip nulls
 */
export function extractComplexAspects(mcps: Mcp[]): ComplexAspect[] {
  const results: ComplexAspect[] = [];

  for (const mcp of mcps) {
    if (!mcp.proposedSnapshot) continue;
    const snapshot = mcp.proposedSnapshot as Record<string, { urn: string; aspects: Record<string, unknown>[] }>;

    for (const snapVal of Object.values(snapshot)) {
      const urn = snapVal.urn;
      if (!urn || !Array.isArray(snapVal.aspects)) continue;

      for (const aspect of snapVal.aspects) {
        const keys = Object.keys(aspect);

        if (urn.startsWith('urn:li:dataset:')) {
          const siblingsKey = keys.find((k) => k.includes('Siblings'));
          if (siblingsKey) {
            results.push({ urn, entityType: 'dataset', aspectName: 'siblings', value: aspect[siblingsKey] });
            continue;
          }
          const lineageKey = keys.find((k) => k.includes('UpstreamLineage'));
          if (lineageKey) {
            results.push({ urn, entityType: 'dataset', aspectName: 'upstreamLineage', value: aspect[lineageKey] });
            continue;
          }
          const schemaKey = keys.find((k) => k === 'com.linkedin.schema.SchemaMetadata');
          if (schemaKey) {
            const schema = stripNulls(aspect[schemaKey]) as Record<string, unknown>;
            if (!schema || !Array.isArray(schema.fields) || schema.fields.length === 0) continue;
            // Replace KafkaSchema/SqlSchema (Avro union types rejected by MCP) with OtherSchema.
            schema.platformSchema = { 'com.linkedin.schema.OtherSchema': { rawSchema: 'schema' } };
            results.push({ urn, entityType: 'dataset', aspectName: 'schemaMetadata', value: schema });
          }
        } else if (urn.startsWith('urn:li:chart:')) {
          const chartKey = keys.find((k) => k.includes('ChartInfo'));
          if (chartKey) {
            const rawInfo = aspect[chartKey] as Record<string, unknown>;
            const info = stripNulls(rawInfo) as Record<string, unknown>;
            const rawInputs = rawInfo.inputs;
            if (rawInputs !== null && rawInputs !== undefined) {
              let inputArr: unknown[];
              if (Array.isArray(rawInputs)) {
                inputArr = rawInputs;
              } else if (typeof rawInputs === 'object' && Array.isArray((rawInputs as Record<string, unknown>).array)) {
                inputArr = (rawInputs as Record<string, unknown>).array as unknown[];
              } else {
                inputArr = [];
              }
              // MCP endpoint requires Avro union format: [{"string": "urn"}]
              info.inputs = inputArr
                .filter((v) => v !== null && v !== undefined)
                .map((v) => (typeof v === 'string' ? { string: v } : v));
            }
            results.push({ urn, entityType: 'chart', aspectName: 'chartInfo', value: info });
          }
        } else if (urn.startsWith('urn:li:dashboard:')) {
          const dashKey = keys.find((k) => k.includes('DashboardInfo'));
          if (dashKey) {
            results.push({
              urn,
              entityType: 'dashboard',
              aspectName: 'dashboardInfo',
              value: stripNulls(aspect[dashKey]),
            });
          }
        } else if (urn.startsWith('urn:li:dataJob:')) {
          const infoKey = keys.find((k) => k.includes('DataJobInfo'));
          if (infoKey) {
            const info = stripNulls(aspect[infoKey]) as Record<string, unknown>;
            // type is an Avro union enum — plain strings are rejected; must be {"string": "VALUE"}
            if (info.type !== null && info.type !== undefined && typeof info.type === 'string') {
              info.type = { string: info.type };
            }
            results.push({ urn, entityType: 'dataJob', aspectName: 'dataJobInfo', value: info });
          }
          const ioKey = keys.find((k) => k.includes('DataJobInputOutput'));
          if (ioKey) {
            results.push({
              urn,
              entityType: 'dataJob',
              aspectName: 'dataJobInputOutput',
              value: stripNulls(aspect[ioKey]),
            });
          }
        } else if (urn.startsWith('urn:li:dataFlow:')) {
          const flowKey = keys.find((k) => k.includes('DataFlowInfo'));
          if (flowKey) {
            results.push({
              urn,
              entityType: 'dataFlow',
              aspectName: 'dataFlowInfo',
              value: stripNulls(aspect[flowKey]),
            });
          }
        } else if (urn.startsWith('urn:li:mlFeature:')) {
          const propsKey = keys.find((k) => k.includes('MLFeatureProperties'));
          if (propsKey) {
            results.push({
              urn,
              entityType: 'mlFeature',
              aspectName: 'mlFeatureProperties',
              value: stripNulls(aspect[propsKey]),
            });
          }
        } else if (urn.startsWith('urn:li:mlPrimaryKey:')) {
          const propsKey = keys.find((k) => k.includes('MLPrimaryKeyProperties'));
          if (propsKey) {
            results.push({
              urn,
              entityType: 'mlPrimaryKey',
              aspectName: 'mlPrimaryKeyProperties',
              value: stripNulls(aspect[propsKey]),
            });
          }
        } else if (urn.startsWith('urn:li:mlModel:')) {
          const propsKey = keys.find((k) => k.includes('MLModelProperties'));
          if (propsKey) {
            results.push({
              urn,
              entityType: 'mlModel',
              aspectName: 'mlModelProperties',
              value: stripNulls(aspect[propsKey]),
            });
          }
        } else if (urn.startsWith('urn:li:mlModelGroup:')) {
          const propsKey = keys.find((k) => k.includes('MLModelGroupProperties'));
          if (propsKey) {
            results.push({
              urn,
              entityType: 'mlModelGroup',
              aspectName: 'mlModelGroupProperties',
              value: stripNulls(aspect[propsKey]),
            });
          }
        }
      }
    }
  }

  return results;
}

/**
 * Post complex aspects extracted from a data.json parse to the GMS
 * /aspects?action=ingestProposal endpoint. Failures are logged but do not throw.
 */
export async function ingestComplexAspects(
  apiContext: APIRequestContext,
  gmsToken: string,
  aspects: ComplexAspect[],
  logger?: DataHubLogger,
): Promise<void> {
  const url = `${gmsUrl()}/aspects?action=ingestProposal`;

  for (const { urn, entityType, aspectName, value } of aspects) {
    const response = await apiContext.post(url, {
      data: {
        proposal: {
          entityType,
          entityUrn: urn,
          changeType: 'UPSERT',
          aspectName,
          aspect: {
            contentType: 'application/json',
            value: JSON.stringify(value),
          },
        },
      },
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${gmsToken}`,
      },
      failOnStatusCode: false,
    });

    if (!response.ok()) {
      const body = await response.text();
      logger?.warn('ingestComplexAspects: failed', {
        urn,
        aspectName,
        status: response.status(),
        body: body.slice(0, 200),
      });
    } else {
      logger?.info('ingestComplexAspects: ingested', { urn, aspectName });
    }
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Extract the entity URN from an MCP, supporting both the newer `entityUrn`
 * field and the older `proposedSnapshot` envelope format.
 */
export function extractUrn(mcp: Mcp): Urn {
  if (mcp.entityUrn) return mcp.entityUrn;
  if (mcp.proposedSnapshot) {
    const snapshot = Object.values(mcp.proposedSnapshot)[0];
    if (snapshot?.urn) return snapshot.urn;
  }
  throw new Error(`Unable to extract URN from MCP: ${JSON.stringify(mcp).slice(0, 120)}`);
}

/**
 * Poll the GMS REST entities endpoint until each URN is reachable or `timeout`
 * elapses. Throws if any entity is still absent after the deadline.
 *
 * Using the REST endpoint (rather than GraphQL) keeps this utility independent
 * of entity type and avoids GraphQL schema differences across DataHub versions.
 */
export async function waitForSync(
  request: APIRequestContext,
  gmsUrl: string,
  urns: Urn[],
  gmsToken?: string,
  timeout: number = 30_000,
): Promise<void> {
  const headers: Record<string, string> = gmsToken ? { Authorization: `Bearer ${gmsToken}` } : {};

  const deadline = Date.now() + timeout;

  for (const urn of urns) {
    let found = false;

    while (!found && Date.now() < deadline) {
      try {
        const response = await request.get(`${gmsUrl}/entities/${encodeURIComponent(urn)}`, {
          headers,
          failOnStatusCode: false,
        });
        if (response.ok()) {
          found = true;
          continue;
        }
      } catch {
        // entity not yet indexed — retry
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 500));
    }

    if (!found) {
      throw new Error(`Timeout: entity ${urn} not indexed after ${timeout}ms`);
    }
  }

  // Extra buffer to allow the search index to catch up.
  await new Promise<void>((resolve) => setTimeout(resolve, 2_000));
}
