/**
 * Dynamic time-relative data seeding for lineage V3 tests.
 *
 * DATA CREATED HERE:
 * - Datasets and jobs with runtime-computed timestamps
 * - Lineage edges with specific created/modified times relative to "now"
 * - Used to test time-range filtering
 *
 * DATA CREATED IN fixtures/data.json:
 * - Static reference datasets (auto-seeded via Playwright fixture)
 *
 * Why not static JSON? Timestamps must be relative to "now" to test time-range filtering.
 */

import type { APIRequestContext } from '@playwright/test';
import { gmsUrl } from '../../utils/constants';
import type { DataHubLogger } from '../../utils/logger';

/** Compute a timestamp N days in the past (for time-range filter tests). */
function daysAgoMs(days: number): number {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

/** Emit a single MCP via REST API. Low-level operation used by all seeding functions. */
async function ingestProposal(
  request: APIRequestContext,
  gmsToken: string,
  entityType: string,
  entityUrn: string,
  aspectName: string,
  aspectValue: unknown,
  logger?: DataHubLogger,
): Promise<void> {
  const url = `${gmsUrl()}/aspects?action=ingestProposal`;
  const payload = {
    proposal: {
      entityType,
      entityUrn,
      changeType: 'UPSERT',
      aspectName,
      aspect: {
        contentType: 'application/json',
        value: JSON.stringify(aspectValue),
      },
    },
  };

  const response = await request.post(url, {
    data: payload,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${gmsToken}`,
    },
  });

  if (!response.ok()) {
    const body = await response.text();
    logger?.warn('Failed to ingest aspect', { entityUrn, aspectName, status: response.status(), body });
  } else {
    logger?.info('Ingested aspect', { entityUrn, aspectName });
  }
}

/** Create a dataset stub. Simple entities without schema (use fixtures/data.json for full schema). */
async function seedDatasetStub(
  request: APIRequestContext,
  gmsToken: string,
  urn: string,
  name: string,
  logger?: DataHubLogger,
): Promise<void> {
  await ingestProposal(request, gmsToken, 'dataset', urn, 'datasetProperties', { name }, logger);
}

/** Create a data job for job→dataset lineage. */
async function seedDataJobStub(
  request: APIRequestContext,
  gmsToken: string,
  urn: string,
  name: string,
  flowUrn: string,
  logger?: DataHubLogger,
): Promise<void> {
  await ingestProposal(
    request,
    gmsToken,
    'dataJob',
    urn,
    'dataJobInfo',
    { name, description: name, type: { string: 'BATCH' }, flowUrn, customProperties: {} },
    logger,
  );
}

async function seedDataFlowStub(
  request: APIRequestContext,
  gmsToken: string,
  urn: string,
  name: string,
  logger?: DataHubLogger,
): Promise<void> {
  await ingestProposal(
    request,
    gmsToken,
    'dataFlow',
    urn,
    'dataFlowInfo',
    { name, description: name, customProperties: {} },
    logger,
  );
}

/** Lineage edge with timestamps that control visibility during time-range filtering. */
interface Edge {
  destinationUrn: string;
  created: { time: number; actor: string };
  lastModified: { time: number; actor: string };
}

/** Create lineage edge with specific creation/update timestamps. */
function makeEdge(destinationUrn: string, createdMs: number, updatedMs: number): Edge {
  return {
    destinationUrn,
    created: { time: createdMs, actor: 'urn:li:corpuser:datahub' },
    lastModified: { time: updatedMs, actor: 'urn:li:corpuser:datahub' },
  };
}

/** Create job→dataset edges with time-relative visibility for lineage changes. */
async function seedDataJobInputOutput(
  request: APIRequestContext,
  gmsToken: string,
  jobUrn: string,
  inputEdges: Edge[],
  outputEdges: Edge[],
  logger?: DataHubLogger,
): Promise<void> {
  await ingestProposal(
    request,
    gmsToken,
    'dataJob',
    jobUrn,
    'dataJobInputOutput',
    {
      inputDatasets: [],
      outputDatasets: [],
      inputDatasetEdges: inputEdges,
      outputDatasetEdges: outputEdges,
    },
    logger,
  );
}

/** Create dataset→dataset edges with time-relative visibility. */
async function seedUpstreamLineage(
  request: APIRequestContext,
  gmsToken: string,
  datasetUrn: string,
  upstreamDatasets: Array<{ urn: string; createdMs: number; updatedMs: number }>,
  logger?: DataHubLogger,
): Promise<void> {
  const upstreams = upstreamDatasets.map((u) => ({
    dataset: u.urn,
    type: 'TRANSFORMED',
    auditStamp: { time: u.updatedMs, actor: 'urn:li:corpuser:datahub' },
    created: { time: u.createdMs, actor: 'urn:li:corpuser:datahub' },
  }));

  await ingestProposal(request, gmsToken, 'dataset', datasetUrn, 'upstreamLineage', { upstreams }, logger);
}

export {
  ingestProposal,
  seedDatasetStub,
  seedDataJobStub,
  seedDataFlowStub,
  seedDataJobInputOutput,
  seedUpstreamLineage,
  makeEdge,
  daysAgoMs,
};
