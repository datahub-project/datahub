/**
 * Large-dataset seeder: one wide dataset whose every column carries a description, a
 * handful of glossary terms and a handful of structured properties, all drawn from pools
 * created alongside it.
 *
 * Everything is derived from a fixed PRNG seed, so a given `LargeDatasetOptions` yields
 * the identical dataset on every run and machine, and callers can assert on specific
 * columns and values. Seeding goes through the OpenAPI v3 batch endpoint so a
 * 2000-column dataset takes a handful of requests rather than thousands. Re-running is
 * cheap: `ensureLargeDataset` checks the server first and only seeds what is missing.
 *
 * Usage from a spec:
 *   const large = await ensureLargeDataset(request, gmsUrl(), gmsToken, { columns: 2000 });
 *   await page.goto(`/dataset/${encodeURIComponent(large.datasetUrn)}/Schema`);
 */
import type { APIRequestContext } from '@playwright/test';

export interface LargeDatasetOptions {
  /** Number of schema fields. */
  columns: number;
  /** PRNG seed; change it to get a different (but still deterministic) dataset. */
  seed?: number;
  /** Size of the structured-property pool and how many each column gets. */
  propertyPool?: number;
  propertiesPerField?: number;
  /** Size of the glossary-term pool and how many each column gets. */
  termPool?: number;
  termsPerField?: number;
}

const DEFAULTS = {
  seed: 20260916,
  propertyPool: 30,
  propertiesPerField: 5,
  termPool: 20,
  termsPerField: 5,
} as const;

export const PLATFORM_URN = 'urn:li:dataPlatform:snowflake';

/** Every option that shapes the generated data is part of the dataset's identity, so two
 *  differently-configured fixtures can never be mistaken for one another on the server. */
export const largeDatasetName = (o: Required<LargeDatasetOptions>): string =>
  `wide_table_${o.columns}c_${o.propertiesPerField}of${o.propertyPool}p_${o.termsPerField}of${o.termPool}t_s${o.seed}`;
export const largeDatasetUrn = (o: Required<LargeDatasetOptions>): string =>
  `urn:li:dataset:(${PLATFORM_URN},scale_smoke.schema_tab.${largeDatasetName(o)},PROD)`;

function validateOptions(o: Required<LargeDatasetOptions>): void {
  const problems: string[] = [];
  if (!Number.isInteger(o.columns) || o.columns < 1)
    problems.push(`columns must be a positive integer (got ${o.columns})`);
  if (o.propertiesPerField > o.propertyPool)
    problems.push(`propertiesPerField (${o.propertiesPerField}) exceeds propertyPool (${o.propertyPool})`);
  if (o.termsPerField > o.termPool)
    problems.push(`termsPerField (${o.termsPerField}) exceeds termPool (${o.termPool})`);
  if (o.propertiesPerField < 0 || o.termsPerField < 0) problems.push('per-field counts must not be negative');
  if (problems.length) throw new Error(`large dataset options invalid: ${problems.join('; ')}`);
}

const ADJECTIVES = [
  'primary',
  'secondary',
  'legacy',
  'derived',
  'raw',
  'normalized',
  'effective',
  'billing',
  'shipping',
  'source',
  'target',
  'rolling',
  'daily',
  'weekly',
  'gross',
  'net',
  'adjusted',
  'estimated',
  'actual',
  'projected',
  'regional',
  'global',
  'internal',
  'external',
];
const NOUNS = [
  'customer_id',
  'order_total',
  'created_at',
  'updated_at',
  'status',
  'region_code',
  'currency',
  'unit_price',
  'quantity',
  'discount_pct',
  'tax_amount',
  'sku',
  'warehouse',
  'carrier',
  'tracking_number',
  'email',
  'phone',
  'postal_code',
  'segment',
  'channel',
  'campaign',
  'cohort',
  'ltv',
  'churn_score',
  'margin',
  'revenue',
  'cost',
  'flag',
];
const VERBS = ['Captures', 'Stores', 'Tracks', 'Records', 'Holds', 'Derives', 'Reports'];
const OBJECTS = [
  'the value as delivered by the upstream feed',
  'the latest known state for the row',
  'a nightly snapshot taken after reconciliation',
  'the amount in the account currency',
  'the identifier assigned by the source system',
  'a normalized code from the reference table',
  'the timestamp in UTC with millisecond precision',
];
const CAVEATS = [
  'Nullable for records created before the 2024 migration.',
  'Populated by the enrichment job; may lag by up to one hour.',
  'Deprecated in favour of the v2 column but still read by finance.',
  'Backfilled from the archive for historical rows.',
  'Validated against the reference data set on ingest.',
  '',
];
const FIELD_TYPES = [
  { pegasus: 'com.linkedin.schema.StringType', native: 'VARCHAR' },
  { pegasus: 'com.linkedin.schema.NumberType', native: 'NUMBER(38,0)' },
  { pegasus: 'com.linkedin.schema.BooleanType', native: 'BOOLEAN' },
  { pegasus: 'com.linkedin.schema.DateType', native: 'TIMESTAMP_NTZ' },
] as const;

/** mulberry32: tiny, fast, deterministic. */
function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const pick = <T>(rnd: () => number, items: readonly T[]): T => items[Math.floor(rnd() * items.length)];

/** k distinct indexes from [0, n). */
function sample(rnd: () => number, n: number, k: number): number[] {
  const pool = Array.from({ length: n }, (_, i) => i);
  for (let i = 0; i < k; i += 1) {
    const j = i + Math.floor(rnd() * (n - i));
    [pool[i], pool[j]] = [pool[j], pool[i]];
  }
  return pool.slice(0, k).sort((a, b) => a - b);
}

export interface ScaleProperty {
  urn: string;
  qualifiedName: string;
  displayName: string;
}
export interface ScaleTerm {
  urn: string;
  name: string;
}
export interface ScaleField {
  fieldPath: string;
  description: string;
  type: (typeof FIELD_TYPES)[number];
  termUrns: string[];
  /** Property → value assigned to this column. */
  properties: { property: ScaleProperty; value: string }[];
}
export interface LargeDataset {
  datasetUrn: string;
  datasetName: string;
  seed: number;
  properties: ScaleProperty[];
  terms: ScaleTerm[];
  fields: ScaleField[];
}

/** Pure: describes the dataset without touching the server. */
export function buildLargeDataset(options: LargeDatasetOptions): LargeDataset {
  const o: Required<LargeDatasetOptions> = { ...DEFAULTS, ...options };
  validateOptions(o);
  const rnd = prng(o.seed);

  const properties: ScaleProperty[] = Array.from({ length: o.propertyPool }, (_, i) => {
    const qualifiedName = `scale_smoke.prop_${String(i).padStart(2, '0')}`;
    return { urn: `urn:li:structuredProperty:${qualifiedName}`, qualifiedName, displayName: `Scale Prop ${i}` };
  });

  const terms: ScaleTerm[] = Array.from({ length: o.termPool }, (_, i) => {
    const name = `ScaleTerm${String(i).padStart(2, '0')}`;
    return { urn: `urn:li:glossaryTerm:scale_smoke.${name}`, name };
  });

  const fields: ScaleField[] = Array.from({ length: o.columns }, (_, i) => {
    const fieldPath = `${pick(rnd, ADJECTIVES)}_${pick(rnd, NOUNS)}_${String(i).padStart(4, '0')}`;
    const description = `${pick(rnd, VERBS)} ${pick(rnd, OBJECTS)} for ${fieldPath}. ${pick(rnd, CAVEATS)}`.trim();
    const type = pick(rnd, FIELD_TYPES);
    const termUrns = sample(rnd, o.termPool, o.termsPerField).map((t) => terms[t].urn);
    const props = sample(rnd, o.propertyPool, o.propertiesPerField).map((p) => ({
      property: properties[p],
      value: `${properties[p].qualifiedName.split('.')[1]}_v${Math.floor(rnd() * 1000)}`,
    }));
    return { fieldPath, description, type, termUrns, properties: props };
  });

  return {
    datasetUrn: largeDatasetUrn(o),
    datasetName: largeDatasetName(o),
    seed: o.seed,
    properties,
    terms,
    fields,
  };
}

export const schemaFieldUrn = (datasetUrn: string, fieldPath: string): string =>
  `urn:li:schemaField:(${datasetUrn},${fieldPath})`;

// ── Seeding ──────────────────────────────────────────────────────────────────

type V3Entity = { urn: string } & Record<string, unknown>;

/**
 * Upserts entities through `POST /openapi/v3/entity/{entityName}` in chunks.
 * Each element is `{ urn, <aspectName>: { value } }`.
 */
export class LargeDatasetSeeder {
  private readonly headers: Record<string, string>;

  constructor(
    private readonly request: APIRequestContext,
    private readonly gmsUrl: string,
    gmsToken: string,
  ) {
    this.headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${gmsToken}` };
  }

  /** True when the dataset already has this many columns and the last one carries its properties. */
  async alreadySeeded(fixture: LargeDataset): Promise<boolean> {
    const dataset = await this.request.get(
      `${this.gmsUrl}/openapi/v3/entity/dataset/${encodeURIComponent(fixture.datasetUrn)}?aspects=schemaMetadata`,
      { headers: this.headers, failOnStatusCode: false },
    );
    if (!dataset.ok()) return false;
    const body = (await dataset.json()) as { schemaMetadata?: { value?: { fields?: unknown[] } } };
    if (body.schemaMetadata?.value?.fields?.length !== fixture.fields.length) return false;

    const last = fixture.fields[fixture.fields.length - 1];
    const field = await this.request.get(
      `${this.gmsUrl}/openapi/v3/entity/schemaField/${encodeURIComponent(
        schemaFieldUrn(fixture.datasetUrn, last.fieldPath),
      )}?aspects=structuredProperties`,
      { headers: this.headers, failOnStatusCode: false },
    );
    if (!field.ok()) return false;
    const fieldBody = (await field.json()) as { structuredProperties?: { value?: { properties?: unknown[] } } };
    return fieldBody.structuredProperties?.value?.properties?.length === last.properties.length;
  }

  async seed(fixture: LargeDataset): Promise<void> {
    const now = Date.now();
    const auditStamp = { time: now, actor: 'urn:li:corpuser:datahub' };

    await this.upsert(
      'structuredProperty',
      fixture.properties.map((p) => ({
        urn: p.urn,
        propertyDefinition: {
          value: {
            qualifiedName: p.qualifiedName,
            displayName: p.displayName,
            description: `Scale smoke property ${p.displayName}`,
            valueType: 'urn:li:dataType:datahub.string',
            cardinality: 'SINGLE',
            entityTypes: ['urn:li:entityType:datahub.schemaField'],
          },
        },
        structuredPropertySettings: {
          value: {
            isHidden: false,
            showInSearchFilters: false,
            showInAssetSummary: true,
            showAsAssetBadge: false,
            showInColumnsTable: false,
          },
        },
      })),
    );

    await this.upsert(
      'glossaryTerm',
      fixture.terms.map((t) => ({
        urn: t.urn,
        glossaryTermInfo: {
          value: { name: t.name, definition: `Scale smoke glossary term ${t.name}`, termSource: 'INTERNAL' },
        },
      })),
    );

    await this.upsert('dataset', [
      {
        urn: fixture.datasetUrn,
        datasetProperties: {
          value: {
            name: fixture.datasetName,
            description: `Scale smoke dataset with ${fixture.fields.length} columns`,
            customProperties: { generated_by: 'e2e large-dataset-seeder', seed: String(fixture.seed) },
          },
        },
        schemaMetadata: {
          value: {
            schemaName: fixture.datasetName,
            platform: PLATFORM_URN,
            version: 0,
            hash: '',
            platformSchema: { 'com.linkedin.schema.OtherSchema': { rawSchema: '' } },
            fields: fixture.fields.map((f) => ({
              fieldPath: f.fieldPath,
              nullable: true,
              recursive: false,
              description: f.description,
              type: { type: { [f.type.pegasus]: {} } },
              nativeDataType: f.type.native,
              glossaryTerms: { terms: f.termUrns.map((urn) => ({ urn })), auditStamp },
            })),
          },
        },
      },
    ]);

    await this.upsert(
      'schemaField',
      fixture.fields.map((f) => ({
        urn: schemaFieldUrn(fixture.datasetUrn, f.fieldPath),
        structuredProperties: {
          value: {
            properties: f.properties.map(({ property, value }) => ({
              propertyUrn: property.urn,
              values: [{ string: value }],
            })),
          },
        },
      })),
      250,
    );
  }

  private async upsert(entityName: string, entities: V3Entity[], chunkSize = 100): Promise<void> {
    for (let i = 0; i < entities.length; i += chunkSize) {
      const chunk = entities.slice(i, i + chunkSize);
      const response = await this.request.post(`${this.gmsUrl}/openapi/v3/entity/${entityName}?async=false`, {
        data: chunk,
        headers: this.headers,
        failOnStatusCode: false,
        timeout: 120_000,
      });
      if (!response.ok()) {
        const body = await response.text();
        throw new Error(
          `scale seed: upsert ${entityName} [${i}..${i + chunk.length}) failed: ${response.status()} ${body.slice(0, 400)}`,
        );
      }
    }
  }
}

/**
 * Build the dataset description and make sure it exists on the server. Set
 * `LARGE_DATASET_RESEED=1` to force a re-seed even when the server already has it.
 */
export async function ensureLargeDataset(
  request: APIRequestContext,
  gmsUrl: string,
  gmsToken: string,
  options: LargeDatasetOptions,
): Promise<{ dataset: LargeDataset; seeded: boolean; seedMs: number }> {
  const dataset = buildLargeDataset(options);
  const seeder = new LargeDatasetSeeder(request, gmsUrl, gmsToken);
  if (process.env.LARGE_DATASET_RESEED !== '1' && (await seeder.alreadySeeded(dataset))) {
    return { dataset, seeded: false, seedMs: 0 };
  }
  const started = Date.now();
  await seeder.seed(dataset);
  return { dataset, seeded: true, seedMs: Date.now() - started };
}
