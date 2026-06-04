/**
 * Time-range lineage seeder for lineage-v2 tests.
 *
 * Seeds DataJob and dataset lineage edges with relative timestamps so that
 * time-range filter tests can verify that edges created at different times
 * appear/disappear correctly in the lineage graph.
 *
 * This module is called from test `beforeAll` hooks because the timestamps
 * must be relative to the current test run time and cannot be stored in
 * static JSON fixture files.
 *
 * Seeded scenarios:
 *
 *   Case 1 – Data job input change (transaction_etl):
 *     transactions   → transaction_etl  (created 8 days ago)
 *     user_profile   → transaction_etl  (created 1 day ago)
 *     transaction_etl → aggregated      (created 8 days ago)
 *
 *   Case 2 – Data job replaced (monthly_temperature):
 *     temperature_etl_1 → monthly_temperature  (created 8 days ago)
 *     temperature_etl_2 → monthly_temperature  (created 1 day ago)
 *
 *   Case 3 – Dataset join change (gnp):
 *     gdp           → gnp  (created 8 days ago, updated 1 day ago)
 *     factor_income → gnp  (created 8 days ago, updated 8 days ago — removed since then)
 */

import type { APIRequestContext } from '@playwright/test';
import { gmsUrl } from './constants';
import type { DataHubLogger } from './logger';

// ── Timestamp helpers ─────────────────────────────────────────────────────────

function daysAgoMs(days: number): number {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

// ── URN constants ─────────────────────────────────────────────────────────────

const TRANSACTIONS_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.transactions,PROD)';
const USER_PROFILE_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)';
const AGGREGATED_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.aggregated_transactions,PROD)';
const TRANSACTION_ETL_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,bq_etl,prod),transaction_etl)';

const DAILY_TEMPERATURE_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)';
const MONTHLY_TEMPERATURE_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.monthly_temperature,PROD)';
const SNOWFLAKE_ETL_FLOW_URN = 'urn:li:dataFlow:(airflow,snowflake_etl,PROD)';
const TEMPERATURE_ETL_1_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,snowflake_etl,PROD),temperature_etl_1)';
const TEMPERATURE_ETL_2_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,snowflake_etl,PROD),temperature_etl_2)';

const GDP_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.gdp,PROD)';
const FACTOR_INCOME_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.factor_income,PROD)';
const GNP_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.gnp,PROD)';

// ── Ingestion helpers ─────────────────────────────────────────────────────────

interface Edge {
  destinationUrn: string;
  created: { time: number; actor: string };
  lastModified: { time: number; actor: string };
}

function makeEdge(destinationUrn: string, createdMs: number, updatedMs: number): Edge {
  return {
    destinationUrn,
    created: { time: createdMs, actor: 'urn:li:corpuser:datahub' },
    lastModified: { time: updatedMs, actor: 'urn:li:corpuser:datahub' },
  };
}

/** Create a minimal dataset stub (datasetProperties only) so the entity is visible in the graph. */
async function seedDatasetStub(
  request: APIRequestContext,
  gmsToken: string,
  urn: string,
  description: string,
  logger?: DataHubLogger,
): Promise<void> {
  await ingestProposal(request, gmsToken, 'dataset', urn, 'datasetProperties', { description }, logger);
}

/** Create a minimal dataFlow stub so the entity is visible. */
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

/** Create a minimal dataJob stub so the entity is visible. */
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
    { name, description: name, type: 'BATCH', flowUrn, customProperties: {} },
    logger,
  );
}

/**
 * Delete a single aspect from an entity using the OpenAPI v3 DELETE endpoint.
 * Used to clear stale graph edges before re-seeding with historical timestamps.
 * A DELETE removes the aspect from MySQL and triggers removal of the corresponding
 * graph edges from Elasticsearch, ensuring the next seed creates fresh edges with
 * correct createdOn timestamps rather than going through mergeEdges (which nulls
 * out createdOn to preserve the original, now-wrong, timestamp).
 */
async function deleteAspect(
  request: APIRequestContext,
  gmsToken: string,
  entityType: string,
  entityUrn: string,
  aspectName: string,
  logger?: DataHubLogger,
): Promise<void> {
  const encodedUrn = encodeURIComponent(entityUrn);
  const url = `${gmsUrl()}/openapi/v3/entity/${entityType}/${encodedUrn}?aspects=${aspectName}`;
  const response = await request.delete(url, {
    headers: {
      Authorization: `Bearer ${gmsToken}`,
    },
    failOnStatusCode: false,
  });

  if (!response.ok() && response.status() !== 404) {
    const body = await response.text();
    logger?.warn('lineage-time-seeder: delete aspect failed', {
      entityUrn,
      aspectName,
      status: response.status(),
      body,
    });
  } else {
    logger?.info('lineage-time-seeder: deleted aspect', { entityUrn, aspectName });
  }
}

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
    failOnStatusCode: false,
  });

  if (!response.ok()) {
    const body = await response.text();
    logger?.warn('lineage-time-seeder: ingest failed', { entityUrn, aspectName, status: response.status(), body });
  } else {
    logger?.info('lineage-time-seeder: ingested', { entityUrn, aspectName });
  }
}

// ── Public seeding entry point ────────────────────────────────────────────────

/**
 * Seed all time-range lineage scenarios.
 * Safe to call multiple times — each call simply overwrites the aspect.
 */
export async function seedTimeRangeLineage(
  request: APIRequestContext,
  gmsToken: string,
  logger?: DataHubLogger,
): Promise<void> {
  const eightDaysAgo = daysAgoMs(8);
  const oneDayAgo = daysAgoMs(1);

  // ── Entity stubs (required for nodes to appear in lineage graph) ──────────
  // Seeds minimal datasetProperties / dataJobInfo / dataFlowInfo so that
  // the lineage graph can render node cards for these entities.

  const BQ_ETL_FLOW_URN = 'urn:li:dataFlow:(airflow,bq_etl,prod)';

  await seedDataFlowStub(request, gmsToken, BQ_ETL_FLOW_URN, 'bq_etl', logger);
  await seedDataFlowStub(request, gmsToken, SNOWFLAKE_ETL_FLOW_URN, 'snowflake_etl', logger);

  await seedDataJobStub(request, gmsToken, TRANSACTION_ETL_URN, 'transaction_etl', BQ_ETL_FLOW_URN, logger);
  await seedDataJobStub(request, gmsToken, TEMPERATURE_ETL_1_URN, 'temperature_etl_1', SNOWFLAKE_ETL_FLOW_URN, logger);
  await seedDataJobStub(request, gmsToken, TEMPERATURE_ETL_2_URN, 'temperature_etl_2', SNOWFLAKE_ETL_FLOW_URN, logger);

  await seedDatasetStub(request, gmsToken, TRANSACTIONS_URN, 'transactions dataset', logger);
  await seedDatasetStub(request, gmsToken, USER_PROFILE_URN, 'user profile dataset', logger);
  await seedDatasetStub(request, gmsToken, AGGREGATED_URN, 'aggregated transactions dataset', logger);
  await seedDatasetStub(request, gmsToken, DAILY_TEMPERATURE_URN, 'daily temperature dataset', logger);
  await seedDatasetStub(request, gmsToken, MONTHLY_TEMPERATURE_URN, 'monthly temperature dataset', logger);
  await seedDatasetStub(request, gmsToken, GDP_URN, 'gdp dataset', logger);
  await seedDatasetStub(request, gmsToken, FACTOR_INCOME_URN, 'factor income dataset', logger);
  await seedDatasetStub(request, gmsToken, GNP_URN, 'gnp dataset', logger);

  // ── Case 1: transaction_etl input datasets change ──────────────────────────
  //
  // The deprecated inputDatasets / outputDatasets fields are required (non-optional) by the
  // DataJobInputOutput schema, so they cannot be omitted. Pass empty arrays to satisfy the
  // schema validation without creating timestamp-less duplicate graph edges.
  //
  // Background: both the deprecated array fields and the Edge-typed fields produce graph edges
  // with the same source+destination+relationshipType key. Because Edge.equals() excludes
  // timestamps, when both are non-empty the deprecated field's edge (createdOn = ingestion time)
  // silently wins in the de-duplication Set, discarding the historical timestamps.
  //
  // Delete before re-seeding so that graph edges are recreated from scratch with the correct
  // historical createdOn. Without the delete, a second run hits mergeEdges(), which nulls out
  // createdOn to preserve the "original" — but already-wrong — timestamp from the first run.

  await deleteAspect(request, gmsToken, 'datajob', TRANSACTION_ETL_URN, 'dataJobInputOutput', logger);
  await ingestProposal(
    request,
    gmsToken,
    'dataJob',
    TRANSACTION_ETL_URN,
    'dataJobInputOutput',
    {
      inputDatasets: [],
      outputDatasets: [],
      inputDatasetEdges: [
        // transactions: existed since 8 days ago (visible in both windows)
        makeEdge(TRANSACTIONS_URN, eightDaysAgo, oneDayAgo),
        // user_profile: added 1 day ago (only in 7-day window)
        makeEdge(USER_PROFILE_URN, oneDayAgo, oneDayAgo),
      ],
      outputDatasetEdges: [makeEdge(AGGREGATED_URN, eightDaysAgo, oneDayAgo)],
    },
    logger,
  );

  // ── Case 2: temperature_etl_1 → temperature_etl_2 replacement ────────────

  // temperature_etl_1: input from daily, output to monthly (created 8 days ago)
  await deleteAspect(request, gmsToken, 'datajob', TEMPERATURE_ETL_1_URN, 'dataJobInputOutput', logger);
  await ingestProposal(
    request,
    gmsToken,
    'dataJob',
    TEMPERATURE_ETL_1_URN,
    'dataJobInputOutput',
    {
      inputDatasets: [],
      outputDatasets: [],
      inputDatasetEdges: [makeEdge(DAILY_TEMPERATURE_URN, eightDaysAgo, eightDaysAgo)],
      outputDatasetEdges: [makeEdge(MONTHLY_TEMPERATURE_URN, eightDaysAgo, eightDaysAgo)],
    },
    logger,
  );

  // temperature_etl_2: input from daily, output to monthly (created 1 day ago)
  await deleteAspect(request, gmsToken, 'datajob', TEMPERATURE_ETL_2_URN, 'dataJobInputOutput', logger);
  await ingestProposal(
    request,
    gmsToken,
    'dataJob',
    TEMPERATURE_ETL_2_URN,
    'dataJobInputOutput',
    {
      inputDatasets: [],
      outputDatasets: [],
      inputDatasetEdges: [makeEdge(DAILY_TEMPERATURE_URN, oneDayAgo, oneDayAgo)],
      outputDatasetEdges: [makeEdge(MONTHLY_TEMPERATURE_URN, oneDayAgo, oneDayAgo)],
    },
    logger,
  );

  // ── Case 3: gnp dataset join change ──────────────────────────────────────

  await ingestProposal(
    request,
    gmsToken,
    'dataset',
    GNP_URN,
    'upstreamLineage',
    {
      upstreams: [
        // gdp: created 8 days ago, auditStamp (updated) 1 day ago — visible in [7d→now] window
        {
          dataset: GDP_URN,
          type: 'TRANSFORMED',
          auditStamp: { time: oneDayAgo, actor: 'urn:li:corpuser:datahub' },
          created: { time: eightDaysAgo, actor: 'urn:li:corpuser:datahub' },
        },
        // factor_income: created 8 days ago, auditStamp (updated) 8 days ago — NOT in [7d→now]
        {
          dataset: FACTOR_INCOME_URN,
          type: 'TRANSFORMED',
          auditStamp: { time: eightDaysAgo, actor: 'urn:li:corpuser:datahub' },
          created: { time: eightDaysAgo, actor: 'urn:li:corpuser:datahub' },
        },
      ],
    },
    logger,
  );

  logger?.info('lineage-time-seeder: all scenarios seeded');
}
