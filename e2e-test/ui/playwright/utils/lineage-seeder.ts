/**
 * Lineage and schema seeder — compensates for legacy /entities?action=ingest failures.
 *
 * The legacy ingest endpoint fails for entities that embed Avro schemas with union
 * types (e.g. ["string"]) in their platformSchema field, or that have null values in
 * optional string fields like jsonPath.  Since many entities in data.json use the
 * legacy DatasetSnapshot format, several aspects never reach GMS:
 *
 *   - upstreamLineage — fails with HTTP 500 "Unknown dereferenced type STRING"
 *   - schemaMetadata  — fails with HTTP 500 "ClassCastException" (null jsonPath fields)
 *   - mlFeatureProperties — fails because description/version are null
 *   - mlPrimaryKeyProperties — fails because version is null
 *   - mlModelProperties — mlFeatures field must be seeded for lineage to work
 *   - mlModelGroupProperties — fails because version is null
 *   - chartInfo (baz1/baz2) — fails because chartUrl/type/access/lastRefreshed are null
 *   - dashboardInfo (playwright_baz) — fails because dashboardUrl/access/lastRefreshed are null
 *   - dataJobInfo — fails because type field is a union requiring {"string": "VALUE"} format
 *   - dataFlowInfo — fails because project field is null
 *
 * This utility extracts these failing aspects from data.json and re-ingests them
 * directly via /aspects?action=ingestProposal, stripping null values first.
 *
 * Additional fixes applied per entity type:
 *
 *   Dataset schemaMetadata:
 *     1. Strip null values from all SchemaField objects (jsonPath: null is rejected).
 *     2. Replace all platformSchema types with OtherSchema to avoid union type errors.
 *
 *   Chart chartInfo:
 *     3. Normalise inputs array to Avro union format [{"string": "urn"}].
 *
 *   DataJob dataJobInfo:
 *     4. Wrap the `type` field in Avro union format {"string": "TYPE_VALUE"}.
 *
 * Called from the seeding.fixture.ts global-data seeder via the LINEAGE_RESEED state file.
 * Safe to call multiple times — each call simply overwrites the aspects.
 */

import * as fs from 'fs';
import * as path from 'path';
import type { APIRequestContext } from '@playwright/test';
import { gmsUrl } from './constants';
import type { DataHubLogger } from './logger';

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Recursively remove keys whose value is null. */
function stripNulls(obj: unknown): unknown {
  if (obj === null || obj === undefined) return undefined;
  if (Array.isArray(obj)) return obj.map(stripNulls).filter((v) => v !== undefined);
  if (typeof obj === 'object') {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      if (v !== null && v !== undefined) {
        result[k] = stripNulls(v);
      }
    }
    return result;
  }
  return obj;
}

interface SeededAspect {
  urn: string;
  aspectName: string;
  value: unknown;
  entityType: string;
}

function extractAspects(dataFilePath: string): SeededAspect[] {
  const raw = fs.readFileSync(dataFilePath, 'utf-8').replace(/com\.linkedin\.pegasus2avro\./g, 'com.linkedin.');
  const entities = JSON.parse(raw) as Array<Record<string, unknown>>;

  const results: SeededAspect[] = [];

  for (const entity of entities) {
    if (!entity.proposedSnapshot) continue;
    const snapshot = entity.proposedSnapshot as Record<string, { urn: string; aspects: Record<string, unknown>[] }>;

    for (const snapVal of Object.values(snapshot)) {
      const urn = snapVal.urn;
      if (!urn || !Array.isArray(snapVal.aspects)) continue;

      for (const aspect of snapVal.aspects) {
        const keys = Object.keys(aspect);

        if (urn.startsWith('urn:li:dataset:')) {
          // ── upstreamLineage ─────────────────────────────────────────────────
          const lineageKey = keys.find((k) => k.includes('UpstreamLineage'));
          if (lineageKey) {
            results.push({ urn, aspectName: 'upstreamLineage', value: aspect[lineageKey], entityType: 'dataset' });
            continue;
          }

          // ── schemaMetadata ──────────────────────────────────────────────────
          const schemaKey = keys.find((k) => k === 'com.linkedin.schema.SchemaMetadata');
          if (schemaKey) {
            const schema = stripNulls(aspect[schemaKey]) as Record<string, unknown>;
            if (!schema || !Array.isArray(schema.fields) || schema.fields.length === 0) continue;

            // Replace any complex platformSchema (KafkaSchema, SqlSchema, etc.) with OtherSchema.
            // The MCP endpoint rejects KafkaSchema (Avro union types) and SqlSchema with the
            // same "DataMap should have at least one entry for a union type" error; using
            // OtherSchema is always safe for test purposes since we only need the schema fields.
            schema.platformSchema = { 'com.linkedin.schema.OtherSchema': { rawSchema: 'schema' } };

            results.push({ urn, aspectName: 'schemaMetadata', value: schema, entityType: 'dataset' });
          }
        } else if (urn.startsWith('urn:li:mlFeature:')) {
          // ── mlFeatureProperties — has nullable description/version fields ───
          const propsKey = keys.find((k) => k.includes('MLFeatureProperties'));
          if (propsKey) {
            const props = stripNulls(aspect[propsKey]);
            results.push({ urn, aspectName: 'mlFeatureProperties', value: props, entityType: 'mlFeature' });
          }
        } else if (urn.startsWith('urn:li:chart:')) {
          // ── chartInfo — has nullable chartUrl/type/access/lastRefreshed ─────
          // The `inputs` field is an Avro union — the MCP endpoint requires each item to be
          // wrapped as {"string": "urn:..."}.  Data.json has two formats:
          //   - plain string array: ["urn:..."]  → must be normalized to [{"string": "urn:..."}]
          //   - {"array": [{"string": "urn:..."}]}  → must be unwrapped to [{"string": "urn:..."}]
          const chartKey = keys.find((k) => k.includes('ChartInfo'));
          if (chartKey) {
            const rawInfo = aspect[chartKey] as Record<string, unknown>;
            const info = stripNulls(rawInfo) as Record<string, unknown>;
            // Normalise inputs to [{string: "urn"}] format.
            const rawInputs = rawInfo.inputs;
            if (rawInputs !== null && rawInputs !== undefined) {
              let inputArr: unknown[];
              if (Array.isArray(rawInputs)) {
                inputArr = rawInputs;
              } else if (typeof rawInputs === 'object' && Array.isArray((rawInputs as Record<string, unknown>).array)) {
                // Unwrap {"array": [...]} Avro wrapper
                inputArr = (rawInputs as Record<string, unknown>).array as unknown[];
              } else {
                inputArr = [];
              }
              // Ensure each item is in {"string": "urn"} union format
              info.inputs = inputArr
                .filter((v) => v !== null && v !== undefined)
                .map((v) => {
                  if (typeof v === 'string') return { string: v };
                  return v; // already {"string": "urn"} or similar union object
                });
            }
            results.push({ urn, aspectName: 'chartInfo', value: info, entityType: 'chart' });
          }
        } else if (urn.startsWith('urn:li:dashboard:')) {
          // ── dashboardInfo — has nullable dashboardUrl/access/lastRefreshed ──
          const dashKey = keys.find((k) => k.includes('DashboardInfo'));
          if (dashKey) {
            const info = stripNulls(aspect[dashKey]);
            results.push({ urn, aspectName: 'dashboardInfo', value: info, entityType: 'dashboard' });
          }
        } else if (urn.startsWith('urn:li:dataJob:')) {
          // ── dataJobInfo — type field is a union requiring {"string": "VALUE"} format ──
          const infoKey = keys.find((k) => k.includes('DataJobInfo'));
          if (infoKey) {
            const rawInfo = aspect[infoKey] as Record<string, unknown>;
            const info = stripNulls(rawInfo) as Record<string, unknown>;
            // Wrap the `type` enum field in Avro union format — plain strings are rejected.
            if (info.type !== null && info.type !== undefined && typeof info.type === 'string') {
              info.type = { string: info.type };
            }
            results.push({ urn, aspectName: 'dataJobInfo', value: info, entityType: 'dataJob' });
          }
          // ── dataJobInputOutput — plain arrays, no transformation needed ──────
          const ioKey = keys.find((k) => k.includes('DataJobInputOutput'));
          if (ioKey) {
            const io = stripNulls(aspect[ioKey]);
            results.push({ urn, aspectName: 'dataJobInputOutput', value: io, entityType: 'dataJob' });
          }
        } else if (urn.startsWith('urn:li:dataFlow:')) {
          // ── dataFlowInfo — project field may be null ────────────────────────
          const flowKey = keys.find((k) => k.includes('DataFlowInfo'));
          if (flowKey) {
            const info = stripNulls(aspect[flowKey]);
            results.push({ urn, aspectName: 'dataFlowInfo', value: info, entityType: 'dataFlow' });
          }
        } else if (urn.startsWith('urn:li:mlPrimaryKey:')) {
          // ── mlPrimaryKeyProperties — has nullable version field ─────────────
          const propsKey = keys.find((k) => k.includes('MLPrimaryKeyProperties'));
          if (propsKey) {
            const props = stripNulls(aspect[propsKey]);
            results.push({ urn, aspectName: 'mlPrimaryKeyProperties', value: props, entityType: 'mlPrimaryKey' });
          }
        } else if (urn.startsWith('urn:li:mlModel:')) {
          // ── mlModelProperties — mlFeatures establishes lineage to mlFeature entities ──
          const propsKey = keys.find((k) => k.includes('MLModelProperties'));
          if (propsKey) {
            const props = stripNulls(aspect[propsKey]);
            results.push({ urn, aspectName: 'mlModelProperties', value: props, entityType: 'mlModel' });
          }
        } else if (urn.startsWith('urn:li:mlModelGroup:')) {
          // ── mlModelGroupProperties — has nullable version field ──────────────
          const propsKey = keys.find((k) => k.includes('MLModelGroupProperties'));
          if (propsKey) {
            const props = stripNulls(aspect[propsKey]);
            results.push({ urn, aspectName: 'mlModelGroupProperties', value: props, entityType: 'mlModelGroup' });
          }
        }
      }
    }
  }

  return results;
}

async function ingestAspect(
  request: APIRequestContext,
  gmsToken: string,
  { urn, aspectName, value, entityType }: SeededAspect,
  logger?: DataHubLogger,
): Promise<void> {
  const url = `${gmsUrl()}/aspects?action=ingestProposal`;
  const payload = {
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
    logger?.warn('lineage-seeder: ingest failed', {
      urn,
      aspectName,
      status: response.status(),
      body: body.slice(0, 200),
    });
  } else {
    logger?.info('lineage-seeder: ingested', { urn, aspectName });
  }
}

// ── Public entry point ─────────────────────────────────────────────────────────

/**
 * Seed aspects that the legacy /entities?action=ingest endpoint fails to store.
 *
 * Covers:
 *   - dataset upstreamLineage (fails with HTTP 500 on Avro union types)
 *   - dataset schemaMetadata (fails on null jsonPath / KafkaSchema/SqlSchema union types)
 *   - mlFeatureProperties (fails on null description/version fields)
 *   - mlPrimaryKeyProperties (fails on null version field; establishes sources→dataset lineage)
 *   - mlModelProperties (mlFeatures field establishes lineage to mlFeature entities)
 *   - mlModelGroupProperties (fails on null version field)
 *   - chartInfo (fails on null fields + inputs union type)
 *   - dashboardInfo (fails on null dashboardUrl/access/lastRefreshed fields)
 *   - dataJobInfo (fails because type field is a union requiring {"string": "VALUE"} format)
 *   - dataJobInputOutput (re-ingested for completeness)
 *   - dataFlowInfo (fails on null project field)
 *
 * All aspects are stripped of null values before posting to the MCP endpoint.
 * Safe to call multiple times — each call simply overwrites the aspects.
 */
export async function seedLineageFromDataJson(
  request: APIRequestContext,
  gmsToken: string,
  logger?: DataHubLogger,
): Promise<void> {
  const dataFile = path.join(__dirname, '../test-data/data.json');
  if (!fs.existsSync(dataFile)) {
    logger?.warn('lineage-seeder: data file not found', { dataFile });
    return;
  }

  const aspects = extractAspects(dataFile);
  logger?.info('lineage-seeder: seeding aspects', { count: aspects.length });

  for (const aspect of aspects) {
    await ingestAspect(request, gmsToken, aspect, logger);
  }

  logger?.info('lineage-seeder: done');
}
