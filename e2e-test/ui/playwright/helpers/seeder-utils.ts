/**
 * Shared utilities for test data seeders.
 *
 * extractUrn and waitForSync were duplicated across cli-seeder, graphql-seeder,
 * and rest-seeder. This module is the single canonical source.
 */

import type { APIRequestContext } from '@playwright/test';

export type Urn = string;

// ── Types ─────────────────────────────────────────────────────────────────────

export interface Mcp {
  entityUrn?: string;
  proposedSnapshot?: Record<string, { urn?: string }>;
  [key: string]: unknown;
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
  const headers: Record<string, string> = gmsToken
    ? { Authorization: `Bearer ${gmsToken}` }
    : {};

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
