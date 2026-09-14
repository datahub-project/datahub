/**
 * Search-index hygiene for hard-deleted test entities — through DataHub's own API.
 *
 * A hard delete (`/entities?action=delete`) removes the SQL rows and the entity's search documents,
 * but the index side is asynchronous and can be skipped by a failure or a race. DataHub exposes the
 * remedy as a regular, privilege-gated operations API (MANAGE_SYSTEM_OPERATIONS, nothing debug-only):
 * `POST /openapi/operations/consistency/fix` with the `orphan-index-document` check finds documents
 * whose entity no longer exists in SQL and removes them from every index. Suites call this after
 * their hard deletes so a run leaves the index exactly as it found it.
 */

import type { APIRequestContext } from '@playwright/test';
import { createScriptLogger, type DataHubLogger } from './logger';

const CONSISTENCY_FIX_PATH = '/openapi/operations/consistency/fix';
const ORPHAN_CHECK_ID = 'orphan-index-document';

export interface OrphanSweepSummary {
  /** True when the server has no consistency API (older GMS) — nothing was verified. */
  skipped: boolean;
  entityTypes: string[];
  entitiesScanned: number;
  /** Orphan documents removed. Anything above 0 means a hard delete left residue behind. */
  orphansRemoved: string[];
  failures: string[];
}

interface FixResult {
  entitiesScanned: number;
  issuesFound: number;
  scrollId: string | null;
  entitiesFixed: number;
  entitiesFailed: number;
  fixDetails?: Array<{ urn: string; action: string; success: boolean; errorMessage?: string }>;
}

/** `urn:li:<entityType>:...` → `<entityType>` (dataset, tag, schemaField, structuredProperty, …). */
export function entityTypeOf(urn: string): string {
  const parts = urn.split(':');
  if (parts[0] !== 'urn' || parts[1] !== 'li' || !parts[2]) throw new Error(`not an entity urn: ${urn}`);
  return parts[2];
}

/**
 * Remove any search-index documents left behind by hard-deleted entities of the given URNs' types.
 * Scoped to aspects modified since `sinceEpochMs` so a large catalog isn't scanned end to end.
 */
export async function sweepOrphanIndexDocuments(
  request: APIRequestContext,
  gmsUrl: string,
  urns: string[],
  opts: { sinceEpochMs: number; batchSize?: number; settleMs?: number; logger?: DataHubLogger },
): Promise<OrphanSweepSummary> {
  const logger = opts.logger ?? createScriptLogger('index-consistency');
  const entityTypes = [...new Set(urns.map(entityTypeOf))].sort();
  const summary: OrphanSweepSummary = {
    skipped: false,
    entityTypes,
    entitiesScanned: 0,
    orphansRemoved: [],
    failures: [],
  };

  /** One full scroll of the orphan check for an entity type; `dryRun` reports without deleting. */
  const run = async (
    entityType: string,
    dryRun: boolean,
  ): Promise<{ issues: number; results: FixResult[] } | 'unavailable'> => {
    const results: FixResult[] = [];
    let issues = 0;
    let scrollId: string | null = null;
    do {
      const response = await request.post(`${gmsUrl}${CONSISTENCY_FIX_PATH}`, {
        headers: { 'Content-Type': 'application/json' },
        failOnStatusCode: false,
        data: {
          entityType,
          checkIds: [ORPHAN_CHECK_ID],
          batchSize: opts.batchSize ?? 500,
          scrollId,
          filter: { gePitEpochMs: opts.sinceEpochMs },
          gracePeriodSeconds: 0, // we settle explicitly below instead of excluding recent writes
          dryRun, // server default is a dry run
          async: false, // server default is fire-and-forget; we want the result
        },
      });
      if (response.status() === 404) return 'unavailable';
      if (!response.ok()) {
        throw new Error(
          `${CONSISTENCY_FIX_PATH} (${entityType}) failed: ${response.status()} ${await response.text()}`,
        );
      }
      const result = (await response.json()) as FixResult;
      results.push(result);
      issues += result.issuesFound;
      scrollId = result.scrollId ?? null;
    } while (scrollId);
    return { issues, results };
  };

  for (const entityType of entityTypes) {
    // Index deletes trail the SQL delete asynchronously: give them a moment (dry runs) so that
    // whatever the fix run then removes is genuine residue, not an in-flight delete.
    const deadline = Date.now() + (opts.settleMs ?? 10_000);
    for (;;) {
      const probe = await run(entityType, true);
      if (probe === 'unavailable') {
        logger.warn('consistency API not available on this server — index residue not verified');
        return { ...summary, skipped: true };
      }
      if (probe.issues === 0 || Date.now() > deadline) break;
      await new Promise((r) => setTimeout(r, 1_000));
    }

    const fixed = await run(entityType, false);
    if (fixed === 'unavailable') return { ...summary, skipped: true };
    for (const result of fixed.results) {
      summary.entitiesScanned += result.entitiesScanned;
      for (const d of result.fixDetails ?? []) {
        if (d.success) summary.orphansRemoved.push(d.urn);
        else summary.failures.push(`${d.urn}: ${d.errorMessage ?? d.action}`);
      }
    }
  }

  if (summary.orphansRemoved.length > 0) {
    logger.warn('orphan index documents removed (a hard delete left residue)', {
      urns: summary.orphansRemoved.join(', '),
    });
  } else {
    logger.info('search index clean', { entityTypes: entityTypes.join(','), entitiesScanned: summary.entitiesScanned });
  }
  return summary;
}
