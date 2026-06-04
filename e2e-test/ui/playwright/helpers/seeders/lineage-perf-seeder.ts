/**
 * Synthetic lineage seeders for the perf benchmark.
 *
 * - {@link seedSyntheticLineage}: dataset-only fanout (root + N up + M down)
 *   with optional `schemaMetadata` + per-column `fineGrainedLineages` when
 *   `columnCount > 0`. Drives the dataset and CLL stress matrices.
 * - {@link seedMixedFanoutLineage}: heterogeneous entities (Dataset, DataJob,
 *   DataFlow, Chart, Dashboard) so virt is measured against every edge shape
 *   the renderer supports.
 *
 * Both are idempotent on URN (UPSERT via `/aspects?action=ingestProposal`)
 * and each `scale` gets its own URN namespace, so corpora can coexist.
 * URNs use the dedicated `perfTest` / `airflow` / `looker` platforms for
 * easy identification.
 */

import type { APIRequestContext } from '@playwright/test';

import { gmsUrl } from '../../utils/constants';
import type { DataHubLogger } from '../../utils/logger';

export interface SyntheticLineageOptions {
  /** Short token embedded in every URN — lets multiple corpora coexist. */
  scale: string;
  upstreamCount: number;
  downstreamCount: number;
  /**
   * Columns per dataset. Every dataset gets a `schemaMetadata` aspect with
   * `col_001 … col_NNN`, and each `upstreamLineage` gets matching
   * `fineGrainedLineages` (same-named column on each upstream).
   *
   * Defaults to {@link DEFAULT_SYNTHETIC_COLUMN_COUNT} so the rendered graph
   * actually carries column rows + column edges — without schemas the
   * perf measurement misses the entire CLL render path. Pass `0` to
   * explicitly opt out (dataset-edge cost in isolation).
   */
  columnCount?: number;
  /** Defaults to `perfTest`. */
  platform?: string;
}

/**
 * Modest column count that exercises the column-row + column-edge render
 * machinery without ballooning seed time. The dedicated
 * `LINEAGE_PERF_CLL_MATRIX` covers higher-cardinality stress.
 */
export const DEFAULT_SYNTHETIC_COLUMN_COUNT = 10;

export interface SyntheticLineageHandles {
  rootUrn: string;
  upstreamUrns: string[];
  downstreamUrns: string[];
  /** Column names actually seeded (empty if `columnCount` was 0). */
  columnNames: string[];
}

const DEFAULT_PLATFORM = 'perfTest';
const DEFAULT_FLOW_PLATFORM = 'airflow';
const DEFAULT_BI_PLATFORM = 'looker';
const ACTOR = 'urn:li:corpuser:datahub';

function datasetUrn(platform: string, name: string): string {
  return `urn:li:dataset:(urn:li:dataPlatform:${platform},${name},PROD)`;
}

function dataFlowUrn(orchestrator: string, flowId: string): string {
  return `urn:li:dataFlow:(${orchestrator},${flowId},PROD)`;
}

function dataJobUrn(orchestrator: string, flowId: string, taskId: string): string {
  return `urn:li:dataJob:(${dataFlowUrn(orchestrator, flowId)},${taskId})`;
}

function chartUrn(platform: string, id: string): string {
  return `urn:li:chart:(${platform},${id})`;
}

function dashboardUrn(platform: string, id: string): string {
  return `urn:li:dashboard:(${platform},${id})`;
}

function pad(n: number, width = 4): string {
  return n.toString().padStart(width, '0');
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
    logger?.warn('lineage-perf-seeder: ingest failed', {
      entityUrn,
      aspectName,
      status: response.status(),
      body: body.slice(0, 200),
    });
  }
}

async function seedDatasetStub(
  request: APIRequestContext,
  gmsToken: string,
  urn: string,
  description: string,
  logger?: DataHubLogger,
): Promise<void> {
  await ingestProposal(request, gmsToken, 'dataset', urn, 'datasetProperties', { description }, logger);
}

/**
 * Write a numeric-only `schemaMetadata` aspect with `columnNames` as the
 * fields list. Uses `OtherSchema` for `platformSchema` to avoid the
 * Avro-union types (`SqlSchema`, `KafkaSchema`) that the legacy MCP
 * ingestion endpoint rejects.
 */
async function seedSchemaMetadata(
  request: APIRequestContext,
  gmsToken: string,
  datasetUrn: string,
  platform: string,
  schemaName: string,
  columnNames: string[],
  logger?: DataHubLogger,
): Promise<void> {
  if (columnNames.length === 0) return;
  const now = Date.now();
  const fields = columnNames.map((name) => ({
    fieldPath: name,
    nativeDataType: 'NUMBER',
    type: {
      type: { 'com.linkedin.schema.NumberType': {} },
    },
  }));
  await ingestProposal(
    request,
    gmsToken,
    'dataset',
    datasetUrn,
    'schemaMetadata',
    {
      schemaName,
      platform: `urn:li:dataPlatform:${platform}`,
      version: 0,
      created: { time: now, actor: ACTOR },
      lastModified: { time: now, actor: ACTOR },
      hash: '',
      platformSchema: {
        'com.linkedin.schema.OtherSchema': { rawSchema: '' },
      },
      fields,
    },
    logger,
  );
}

function schemaFieldUrn(datasetUrn: string, columnName: string): string {
  return `urn:li:schemaField:(${datasetUrn},${columnName})`;
}

/** One FGL entry per column, mapping `col` to the same-named column on every upstream. */
function buildFineGrainedLineages(
  downstreamUrn: string,
  upstreamUrns: string[],
  columnNames: string[],
): Array<Record<string, unknown>> {
  return columnNames.map((col) => ({
    upstreamType: 'FIELD_SET',
    downstreamType: 'FIELD_SET',
    upstreams: upstreamUrns.map((u) => schemaFieldUrn(u, col)),
    downstreams: [schemaFieldUrn(downstreamUrn, col)],
    confidenceScore: 1.0,
    transformOperation: 'identity',
  }));
}

/**
 * Write `upstreamLineage` on `entityUrn`. Each entry needs the deprecated
 * `type` field — GMS rejects entries without it. When `columnNames` is non-
 * empty, also emits per-column FGLs (worst-case 1:N fan-in shape).
 */
async function seedUpstreamLineage(
  request: APIRequestContext,
  gmsToken: string,
  entityUrn: string,
  upstreamUrns: string[],
  columnNames: string[],
  logger?: DataHubLogger,
): Promise<void> {
  if (upstreamUrns.length === 0) return;
  const now = Date.now();
  const upstreams = upstreamUrns.map((u) => ({
    dataset: u,
    type: 'TRANSFORMED',
    auditStamp: { time: now, actor: ACTOR },
    created: { time: now, actor: ACTOR },
    lastModified: { time: now, actor: ACTOR },
  }));
  const aspect: Record<string, unknown> = { upstreams };
  if (columnNames.length > 0) {
    aspect.fineGrainedLineages = buildFineGrainedLineages(entityUrn, upstreamUrns, columnNames);
  }
  await ingestProposal(request, gmsToken, 'dataset', entityUrn, 'upstreamLineage', aspect, logger);
}

/**
 * Order: dataset stubs → schemas → lineage. Stub-first guards against future
 * dangling-edge validation; schema-before-lineage ensures FGL column URNs are
 * resolvable when the lineage aspect lands.
 */
export async function seedSyntheticLineage(
  request: APIRequestContext,
  gmsToken: string,
  opts: SyntheticLineageOptions,
  logger?: DataHubLogger,
): Promise<SyntheticLineageHandles> {
  const platform = opts.platform ?? DEFAULT_PLATFORM;
  const columnCount = opts.columnCount ?? DEFAULT_SYNTHETIC_COLUMN_COUNT;
  const columnNames = Array.from({ length: columnCount }, (_, i) => `col_${pad(i + 1, 3)}`);
  const rootUrn = datasetUrn(platform, `perf_${opts.scale}_root`);
  const upstreamUrns = Array.from({ length: opts.upstreamCount }, (_, i) =>
    datasetUrn(platform, `perf_${opts.scale}_up_${pad(i + 1)}`),
  );
  const downstreamUrns = Array.from({ length: opts.downstreamCount }, (_, i) =>
    datasetUrn(platform, `perf_${opts.scale}_down_${pad(i + 1)}`),
  );

  const startMs = Date.now();
  logger?.info('lineage-perf-seeder: seeding synthetic corpus', {
    scale: opts.scale,
    upstreams: opts.upstreamCount,
    downstreams: opts.downstreamCount,
    columns: columnCount,
  });

  await seedDatasetStub(request, gmsToken, rootUrn, `perf root @ scale ${opts.scale}`, logger);
  for (const urn of upstreamUrns) {
    await seedDatasetStub(request, gmsToken, urn, `perf upstream of scale ${opts.scale}`, logger);
  }
  for (const urn of downstreamUrns) {
    await seedDatasetStub(request, gmsToken, urn, `perf downstream of scale ${opts.scale}`, logger);
  }

  if (columnNames.length > 0) {
    await seedSchemaMetadata(request, gmsToken, rootUrn, platform, `perf_${opts.scale}_root`, columnNames, logger);
    for (const urn of upstreamUrns) {
      const name = urn.split(',')[1];
      await seedSchemaMetadata(request, gmsToken, urn, platform, name, columnNames, logger);
    }
    for (const urn of downstreamUrns) {
      const name = urn.split(',')[1];
      await seedSchemaMetadata(request, gmsToken, urn, platform, name, columnNames, logger);
    }
  }

  await seedUpstreamLineage(request, gmsToken, rootUrn, upstreamUrns, columnNames, logger);
  for (const downstreamUrn of downstreamUrns) {
    await seedUpstreamLineage(request, gmsToken, downstreamUrn, [rootUrn], columnNames, logger);
  }

  // OpenSearch indexing is async through MAE/MCE; without this poll, the
  // first run after a fresh seed sees only the root node.
  const expectedMin = Math.max(1, Math.floor(opts.upstreamCount * 0.5));
  await waitForLineageIndexed(request, gmsToken, rootUrn, expectedMin, logger);

  logger?.info('lineage-perf-seeder: done', {
    scale: opts.scale,
    totalEntities: 1 + opts.upstreamCount + opts.downstreamCount,
    columns: columnCount,
    elapsedMs: Date.now() - startMs,
  });

  return { rootUrn, upstreamUrns, downstreamUrns, columnNames };
}

/**
 * Poll `searchAcrossLineage` (the frontend's query) until the root reports at
 * least `expectedMin` upstreams. Bails after `timeoutMs` and lets the test
 * run with whatever is indexed.
 */
async function waitForLineageIndexed(
  request: APIRequestContext,
  gmsToken: string,
  rootUrn: string,
  expectedMin: number,
  logger?: DataHubLogger,
  { timeoutMs = 60_000, pollMs = 2_000 }: { timeoutMs?: number; pollMs?: number } = {},
): Promise<void> {
  // Plain query — only the total count matters for readiness.
  const query = `query LineagePerfProbe($urn: String!) {
    searchAcrossLineage(input: { urn: $urn, direction: UPSTREAM, count: 0, query: "*" }) {
      total
    }
  }`;
  const url = `${gmsUrl()}/api/graphql`;
  const deadline = Date.now() + timeoutMs;
  let lastTotal = -1;
  while (Date.now() < deadline) {
    const response = await request.post(url, {
      data: { query, variables: { urn: rootUrn } },
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${gmsToken}`,
        'X-DataHub-Actor': 'urn:li:corpuser:datahub',
      },
      failOnStatusCode: false,
    });
    if (response.ok()) {
      const body = (await response.json()) as {
        data?: { searchAcrossLineage?: { total?: number } };
      };
      const total = body.data?.searchAcrossLineage?.total ?? 0;
      if (total !== lastTotal) {
        logger?.info('lineage-perf-seeder: waiting for index', { rootUrn, total, expectedMin });
        lastTotal = total;
      }
      if (total >= expectedMin) return;
    }
    await new Promise((resolve) => {
      setTimeout(resolve, pollMs);
    });
  }
  logger?.warn('lineage-perf-seeder: index poll timed out', {
    rootUrn,
    lastTotal,
    expectedMin,
    timeoutMs,
  });
}

// ── Mixed-entity fanout ────────────────────────────────────────────────────
//
// Covers every edge shape the V2 renderer draws: Dataset→Dataset,
// Dataset→DataJob, DataJob→Dataset, Dataset→Chart, Chart→Dashboard.
//
//                          ┌───────────────────────┐
//   upstream datasets ────▶│         root          │──▶ downstream datasets
//                          │      (Dataset)        │──▶ downstream charts ─▶ dashboards
//   jobinput datasets ─▶[DataJob]────────▶          │
//                          └───────────────────────┘
//
// Each DataJob owns its own DataFlow + one input dataset so the
// orchestrator-edge is present and the job isn't dangling.

export interface MixedFanoutOptions {
  scale: string;
  /** Datasets attached to root via root.upstreamLineage. */
  upstreamDatasets: number;
  /** DataJobs whose outputDatasets includes root (each gets own DataFlow + input dataset). */
  upstreamDatajobs: number;
  /** Datasets with root as their upstream. */
  downstreamDatasets: number;
  /** Charts whose chartInfo.inputs includes root. */
  downstreamCharts: number;
  /** Dashboards containing the first N charts (capped at downstreamCharts). */
  downstreamDashboards: number;
  /**
   * Columns per dataset (root + upstream/jobinput/downstream datasets).
   * Defaults to {@link DEFAULT_SYNTHETIC_COLUMN_COUNT} so the rendered graph
   * exercises the column-row + CLL edge render path. Pass `0` to opt out.
   * Note: DataJobs / Charts / Dashboards don't carry `schemaMetadata`, so
   * this only affects dataset nodes.
   */
  columnCount?: number;
  platform?: string;
  flowPlatform?: string;
  biPlatform?: string;
}

export interface MixedFanoutHandles {
  rootUrn: string;
  upstreamDatasetUrns: string[];
  upstreamDatajobUrns: string[];
  upstreamDataflowUrns: string[];
  upstreamJobInputDatasetUrns: string[];
  downstreamDatasetUrns: string[];
  downstreamChartUrns: string[];
  downstreamDashboardUrns: string[];
}

async function seedDataFlow(
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
    { name, description: `perf flow ${name}`, project: null },
    logger,
  );
}

async function seedDataJob(
  request: APIRequestContext,
  gmsToken: string,
  jobUrn: string,
  flowUrn: string,
  name: string,
  inputDatasets: string[],
  outputDatasets: string[],
  logger?: DataHubLogger,
): Promise<void> {
  await ingestProposal(
    request,
    gmsToken,
    'dataJob',
    jobUrn,
    'dataJobInfo',
    { name, description: `perf job ${name}`, type: 'SQL', flowUrn },
    logger,
  );
  await ingestProposal(
    request,
    gmsToken,
    'dataJob',
    jobUrn,
    'dataJobInputOutput',
    { inputDatasets, outputDatasets },
    logger,
  );
}

async function seedChart(
  request: APIRequestContext,
  gmsToken: string,
  urn: string,
  title: string,
  inputs: string[],
  logger?: DataHubLogger,
): Promise<void> {
  const now = Date.now();
  await ingestProposal(
    request,
    gmsToken,
    'chart',
    urn,
    'chartInfo',
    {
      title,
      description: `perf chart ${title}`,
      lastModified: {
        created: { time: now, actor: ACTOR },
        lastModified: { time: now, actor: ACTOR },
      },
      inputs,
    },
    logger,
  );
}

async function seedDashboard(
  request: APIRequestContext,
  gmsToken: string,
  urn: string,
  title: string,
  chartUrns: string[],
  logger?: DataHubLogger,
): Promise<void> {
  const now = Date.now();
  await ingestProposal(
    request,
    gmsToken,
    'dashboard',
    urn,
    'dashboardInfo',
    {
      title,
      description: `perf dashboard ${title}`,
      charts: chartUrns,
      lastModified: {
        created: { time: now, actor: ACTOR },
        lastModified: { time: now, actor: ACTOR },
      },
    },
    logger,
  );
}

export async function seedMixedFanoutLineage(
  request: APIRequestContext,
  gmsToken: string,
  opts: MixedFanoutOptions,
  logger?: DataHubLogger,
): Promise<MixedFanoutHandles> {
  const platform = opts.platform ?? DEFAULT_PLATFORM;
  const flowPlatform = opts.flowPlatform ?? DEFAULT_FLOW_PLATFORM;
  const biPlatform = opts.biPlatform ?? DEFAULT_BI_PLATFORM;
  const columnCount = opts.columnCount ?? DEFAULT_SYNTHETIC_COLUMN_COUNT;
  const columnNames = Array.from({ length: columnCount }, (_, i) => `col_${pad(i + 1, 3)}`);
  const ns = `perf_mx_${opts.scale}`;

  const rootUrn = datasetUrn(platform, `${ns}_root`);
  const upstreamDatasetUrns = Array.from({ length: opts.upstreamDatasets }, (_, i) =>
    datasetUrn(platform, `${ns}_up_ds_${pad(i + 1)}`),
  );
  const upstreamDataflowUrns = Array.from({ length: opts.upstreamDatajobs }, (_, i) =>
    dataFlowUrn(flowPlatform, `${ns}_flow_${pad(i + 1)}`),
  );
  const upstreamDatajobUrns = Array.from({ length: opts.upstreamDatajobs }, (_, i) =>
    dataJobUrn(flowPlatform, `${ns}_flow_${pad(i + 1)}`, `task_${pad(i + 1)}`),
  );
  const upstreamJobInputDatasetUrns = Array.from({ length: opts.upstreamDatajobs }, (_, i) =>
    datasetUrn(platform, `${ns}_jobin_${pad(i + 1)}`),
  );
  const downstreamDatasetUrns = Array.from({ length: opts.downstreamDatasets }, (_, i) =>
    datasetUrn(platform, `${ns}_down_ds_${pad(i + 1)}`),
  );
  const downstreamChartUrns = Array.from({ length: opts.downstreamCharts }, (_, i) =>
    chartUrn(biPlatform, `${ns}_chart_${pad(i + 1)}`),
  );
  const downstreamDashboardUrns = Array.from({ length: opts.downstreamDashboards }, (_, i) =>
    dashboardUrn(biPlatform, `${ns}_dash_${pad(i + 1)}`),
  );

  const totalEntities =
    1 +
    upstreamDatasetUrns.length +
    upstreamDataflowUrns.length +
    upstreamDatajobUrns.length +
    upstreamJobInputDatasetUrns.length +
    downstreamDatasetUrns.length +
    downstreamChartUrns.length +
    downstreamDashboardUrns.length;

  const startMs = Date.now();
  logger?.info('lineage-perf-seeder: seeding mixed fanout', {
    scale: opts.scale,
    upstreamDatasets: opts.upstreamDatasets,
    upstreamDatajobs: opts.upstreamDatajobs,
    downstreamDatasets: opts.downstreamDatasets,
    downstreamCharts: opts.downstreamCharts,
    downstreamDashboards: opts.downstreamDashboards,
    columns: columnCount,
    totalEntities,
  });

  // ── Entity stubs ────────────────────────────────────────────────────────
  await seedDatasetStub(request, gmsToken, rootUrn, `mx root @ ${opts.scale}`, logger);
  for (const urn of upstreamDatasetUrns) {
    await seedDatasetStub(request, gmsToken, urn, `mx upstream dataset @ ${opts.scale}`, logger);
  }
  for (const urn of upstreamJobInputDatasetUrns) {
    await seedDatasetStub(request, gmsToken, urn, `mx job input @ ${opts.scale}`, logger);
  }
  for (const urn of downstreamDatasetUrns) {
    await seedDatasetStub(request, gmsToken, urn, `mx downstream dataset @ ${opts.scale}`, logger);
  }

  // Schemas — emitted on every dataset so the rendered graph actually
  // shows column rows + column edges, matching production lineage.
  if (columnNames.length > 0) {
    const allDatasetUrns = [rootUrn, ...upstreamDatasetUrns, ...upstreamJobInputDatasetUrns, ...downstreamDatasetUrns];
    for (const urn of allDatasetUrns) {
      const name = urn.split(',')[1];
      await seedSchemaMetadata(request, gmsToken, urn, platform, name, columnNames, logger);
    }
  }

  for (let i = 0; i < upstreamDataflowUrns.length; i += 1) {
    await seedDataFlow(request, gmsToken, upstreamDataflowUrns[i], `mx_flow_${pad(i + 1)}`, logger);
  }

  for (let i = 0; i < upstreamDatajobUrns.length; i += 1) {
    await seedDataJob(
      request,
      gmsToken,
      upstreamDatajobUrns[i],
      upstreamDataflowUrns[i],
      `mx_task_${pad(i + 1)}`,
      [upstreamJobInputDatasetUrns[i]],
      [rootUrn],
      logger,
    );
  }

  await seedUpstreamLineage(request, gmsToken, rootUrn, upstreamDatasetUrns, columnNames, logger);
  for (const downstreamUrn of downstreamDatasetUrns) {
    await seedUpstreamLineage(request, gmsToken, downstreamUrn, [rootUrn], columnNames, logger);
  }

  for (let i = 0; i < downstreamChartUrns.length; i += 1) {
    await seedChart(request, gmsToken, downstreamChartUrns[i], `mx_chart_${pad(i + 1)}`, [rootUrn], logger);
  }

  // Each dashboard claims one chart (mod-cycled) so the chart→dashboard
  // edge is exercised.
  for (let i = 0; i < downstreamDashboardUrns.length; i += 1) {
    const chartsForDash = downstreamChartUrns.length ? [downstreamChartUrns[i % downstreamChartUrns.length]] : [];
    await seedDashboard(request, gmsToken, downstreamDashboardUrns[i], `mx_dash_${pad(i + 1)}`, chartsForDash, logger);
  }

  // Wait for OpenSearch to index — both dataset and dataJob upstreams
  // surface under `searchAcrossLineage(UPSTREAM)`.
  const expectedMin = Math.max(1, Math.floor((opts.upstreamDatasets + opts.upstreamDatajobs) * 0.5));
  await waitForLineageIndexed(request, gmsToken, rootUrn, expectedMin, logger);

  logger?.info('lineage-perf-seeder: mixed fanout done', {
    scale: opts.scale,
    totalEntities,
    elapsedMs: Date.now() - startMs,
  });

  return {
    rootUrn,
    upstreamDatasetUrns,
    upstreamDatajobUrns,
    upstreamDataflowUrns,
    upstreamJobInputDatasetUrns,
    downstreamDatasetUrns,
    downstreamChartUrns,
    downstreamDashboardUrns,
  };
}
