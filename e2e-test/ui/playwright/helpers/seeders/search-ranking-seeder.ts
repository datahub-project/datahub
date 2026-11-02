/**
 * Search ranking corpus: a pet-shop catalog shaped to test how keyword search orders results.
 *
 * The catalog has the shapes that break ranking in large deployments:
 *   - many datasets whose display title (a structured property copied into the name) equals the
 *     real name of one dataset, more of them than one result window holds;
 *   - real names that share only a stem with the query ("groom_*" for "groomed");
 *   - a column and a dataset with the same name;
 *   - record identifiers (GUIDs) held in a searchable structured property.
 *
 * {@link matchClass} grades a result against a query from the corpus definition alone, without
 * asking the search engine, so a spec can check the engine's order against the expected order.
 *
 * Seeding:
 *   - Structured properties are created with GraphQL, then their `searchConfiguration` is written
 *     with OpenAPI v3. A Search V3 index maps a property's `.text` sub-field and its `copy_to` when
 *     the index is built, so a property created after that needs a system-update before its values
 *     are searchable as text. {@link rankingPropertiesSearchable} reports whether that has happened.
 *   - Entities are written through the OpenAPI v3 batch endpoint.
 *   - Two search templates are written: a class-order body and a router that inherits the shipped
 *     interactive router and sends ordinary queries to that body. A deployment selects the router
 *     by pointing the search surface at it (see the spec header).
 */
import type { APIRequestContext } from '@playwright/test';
import { waitUntilSearchable } from './search-wait';

export const PLATFORM_URN = 'urn:li:dataPlatform:hive';
const PREFIX = 'rank_req';

export const DISPLAY_TITLE = 'io.example.displayTitle';
export const RECORD_ID = 'io.example.recordId';
export const DISPLAY_TITLE_URN = `urn:li:structuredProperty:${DISPLAY_TITLE}`;
export const RECORD_ID_URN = `urn:li:structuredProperty:${RECORD_ID}`;

export const CLASS_ORDER_TEMPLATE_URN = 'urn:li:searchTemplate:rankingClassOrder';
export const CLASS_ORDER_ROUTER_URN = 'urn:li:searchTemplate:rankingClassOrderSearch';

export const datasetUrn = (name: string): string => `urn:li:dataset:(${PLATFORM_URN},${PREFIX}.${name},PROD)`;
/** A dataset whose key differs from its display name, e.g. a table in a schema named like it. */
export const keyedDatasetUrn = (key: string): string => `urn:li:dataset:(${PLATFORM_URN},${key},PROD)`;
export const columnUrn = (dataset: string, fieldPath: string): string =>
  `urn:li:schemaField:(${datasetUrn(dataset)},${fieldPath})`;

export interface CorpusEntry {
  urn: string;
  kind: 'dataset' | 'column';
  name: string;
  title?: string;
  recordId?: string;
}

const pad = (n: number, width: number): string => String(n).padStart(width, '0');

/** How many datasets carry a copied value equal to one real name: more than the 100-row window. */
export const EQUAL_TITLE_COPIES = Number(process.env.RANKING_EQUAL_TITLE_COPIES ?? 150);
/** Stem-only names for "groomed": more than the 50-row window of a one-word query. */
export const STEM_ONLY_NAMES = Number(process.env.RANKING_STEM_ONLY_NAMES ?? 600);

export const GUID = '3f2b9c1e-7d4a-4b8e-9a61-2c5d8e0f1a7b';
export const OTHER_GUID = '9a7c3e21-55b0-4d1f-8e2a-6b4c0d9f3e18';

const dataset = (name: string, title?: string, recordId?: string): CorpusEntry => ({
  urn: datasetUrn(name),
  kind: 'dataset',
  name,
  title,
  recordId,
});

/** Pure: the whole corpus, deterministic. */
export function buildRankingCorpus(): CorpusEntry[] {
  const entries: CorpusEntry[] = [];
  // A real name, many datasets whose display title equals it, then partial matches of both kinds.
  entries.push(dataset('kennel_booking'));
  for (let i = 0; i < EQUAL_TITLE_COPIES; i += 1) entries.push(dataset(`kb_listing_${pad(i, 3)}`, 'Kennel Booking'));
  for (let i = 0; i < 30; i += 1) entries.push(dataset(`kennel_booking_snapshot_${pad(i, 2)}`));
  for (let i = 0; i < 20; i += 1) entries.push(dataset(`kbx_${pad(i, 2)}`, `Kennel Booking Extract ${i}`));

  // A three-word real name in underscore form, one other dataset titled with the same words, and
  // a name that shares the other two words and only the stem of the first.
  // Near misses for kennel_booking: they hold the words but are not the whole value.
  entries.push(dataset('kennel_booking_archive'));
  entries.push(dataset('kennel_booking_2024'));
  entries.push(dataset('booking_kennel'));
  entries.push(dataset('kennel'));
  entries.push(dataset('booking'));
  entries.push(dataset('kba_ref', 'Kennel Booking Archive'));
  entries.push({ urn: columnUrn('booking_ledger', 'kennel_booking_id'), kind: 'column', name: 'kennel_booking_id' });
  entries.push(dataset('booking_ledger'));

  // The canonical example: "my cat" exact, two partials, two near misses, one equal copied title.
  ['my cat', 'my cat frank', 'awesome my cat', 'my dog', 'cat'].forEach((name) => entries.push(dataset(name)));
  entries.push(dataset('mc_ref', 'My Cat'));

  entries.push(dataset('groomed_coat_notes'));
  entries.push(dataset('gcn_ref', 'Groomed Coat Notes'));
  entries.push(dataset('groom_coat_notes_stage'));

  // "groomed": real names holding the word, one of which also carries it in its own title,
  // titles holding it, and stem-only names.
  entries.push(dataset('salon_audit_groomed'));
  entries.push(dataset('groomed_journal', 'Groomed Journal Entries'));
  for (let i = 0; i < 20; i += 1) entries.push(dataset(`gje_ref_${pad(i, 2)}`, `Groomed Journal Entries ${i}`));
  for (let i = 0; i < STEM_ONLY_NAMES; i += 1)
    entries.push(dataset(`${i % 2 === 0 ? 'groom' : 'grooms'}_${pad(i, 4)}`));

  // A real name equal to its own display title, and other datasets carrying that title.
  // Written after the datasets that only carry the title, so write order cannot put it first.
  for (let i = 0; i < 20; i += 1) entries.push(dataset(`cr_ref_${pad(i, 2)}`, 'Cattery Roster'));
  entries.push(dataset('Cattery Roster', 'Cattery Roster'));

  // A dataset and a column with the same name.
  entries.push(dataset('litter_tray'));
  entries.push(dataset('tray_logs'));
  entries.push({ urn: columnUrn('tray_logs', 'litter_tray'), kind: 'column', name: 'litter_tray' });

  // An asset whose name equals the search, with a longer title of its own, against 12 tables titled
  // exactly the search in a schema named like the table family; and a control with no title.
  entries.push(dataset('ragdoll lounge', 'Ragdoll Lounge Visits'));
  entries.push(dataset('siberian lounge'));
  for (let i = 0; i < 12; i += 1) {
    const n = pad(i, 2);
    entries.push({
      urn: keyedDatasetUrn(`ragdoll_lounge.ragdoll_lounge_${n}`),
      kind: 'dataset',
      name: `ragdoll_lounge_${n}`,
      title: 'Ragdoll Lounge',
    });
    entries.push({
      urn: keyedDatasetUrn(`siberian_lounge.siberian_lounge_${n}`),
      kind: 'dataset',
      name: `siberian_lounge_${n}`,
      title: 'Siberian Lounge',
    });
  }

  // Record identifiers.
  entries.push(dataset('adoption_register', 'Adoption Register', GUID));
  entries.push(dataset('adoption_register_archive', undefined, OTHER_GUID));

  // Name shapes: many-part names in two spellings, an accented name, a leading number.
  entries.push(dataset('a_b_c_d_e_f_g_h'));
  entries.push(dataset('a-b-c-d-e-f-g-h'));
  entries.push(dataset('Café Grooming Orders'));
  entries.push(dataset('2024_kennel_budget'));
  return entries;
}

// ── Grading ──────────────────────────────────────────────────────────────────

/**
 * The tranching rule, best first. Every result in a higher tranche ranks above every result in a
 * lower one, an asset takes its best tranche, and datasets rank above columns of the same match
 * quality. IDENTIFIER is a separate check: a record ID typed as the query. The rule applies to the
 * query as typed, without quotes.
 *
 *   Exact: the whole value equals the query, ignoring case. kennel_booking is exact for
 *   kennel_booking, never for kennel_booking_archive or booking.
 *   Partial: every separator-delimited substring of the query starts one of the value, in any
 *   order, or the value starts with the query. Separators are space, underscore, dot and slash; a
 *   hyphen is not one. There is no stemming, so "groomed" does not start "groom_0001".
 *   SEPARATOR_ONLY: the value differs from the query only in separators ("kennel booking" and
 *   kennel_booking). The rule does not decide where it ranks, so it is left out of comparisons.
 */
export const TRANCHES = [
  'IDENTIFIER',
  'EXACT_NAME',
  'EXACT_COPIED',
  'EXACT_COLUMN',
  'EXACT_COLUMN_COPIED',
  'PARTIAL_NAME',
  'PARTIAL_COPIED',
  'PARTIAL_COLUMN',
  'PARTIAL_COLUMN_COPIED',
  'OTHER',
  'SEPARATOR_ONLY',
] as const;
export type Tranche = (typeof TRANCHES)[number];

/**
 * Separator-delimited substrings, lowercased and accent-folded. Separators are space, underscore,
 * dot and slash; a hyphen is not one, so PET-ADOPT-0042 and a GUID stay one substring.
 */
export const words = (text: string): string[] =>
  text
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .split(/[\s_./]+/)
    .filter(Boolean);

/** The query as typed, without surrounding quotes. */
export const typed = (query: string): string => query.trim().replace(/^"(.+)"$/, '$1');

const exact = (query: string, value?: string): boolean =>
  value !== undefined && typed(query).toLowerCase() === value.trim().toLowerCase();

const sameWords = (a: string[], b: string[]): boolean => a.length === b.length && a.every((w, i) => w === b[i]);

const separatorOnly = (query: string, value?: string): boolean =>
  value !== undefined && !exact(query, value) && sameWords(words(typed(query)), words(value));

const starts = (query: string, value?: string): boolean => {
  if (value === undefined) return false;
  const q = words(typed(query));
  const v = words(value);
  return (
    q.length > 0 &&
    v.length > 0 &&
    (q.every((w) => v.some((x) => x.startsWith(w))) || v.join(' ').startsWith(q.join(' ')))
  );
};

/** The best tranche the entry reaches for the query; the own name is graded before a copied title. */
export function tranche(query: string, entry: CorpusEntry): Tranche {
  if (entry.recordId && entry.recordId.toLowerCase() === typed(query).toLowerCase()) return 'IDENTIFIER';
  const column = entry.kind === 'column';
  if (exact(query, entry.name)) return column ? 'EXACT_COLUMN' : 'EXACT_NAME';
  if (separatorOnly(query, entry.name)) return 'SEPARATOR_ONLY';
  if (exact(query, entry.title)) return column ? 'EXACT_COLUMN_COPIED' : 'EXACT_COPIED';
  if (separatorOnly(query, entry.title)) return 'SEPARATOR_ONLY';
  if (starts(query, entry.name)) return column ? 'PARTIAL_COLUMN' : 'PARTIAL_NAME';
  if (starts(query, entry.title)) return column ? 'PARTIAL_COLUMN_COPIED' : 'PARTIAL_COPIED';
  return 'OTHER';
}

export const trancheRank = (t: Tranche): number => TRANCHES.indexOf(t);

// ── Templates ────────────────────────────────────────────────────────────────

const ref = (property: string, form: '.text' | ''): string => `{{urn:li:structuredProperty:${property}}}${form}`;

/**
 * Class-order body. Each class is a constant score, so the engine sorts every hit by its best
 * class, and a tiny relevance term only breaks ties inside a class. Text clauses read the
 * unstemmed `name.text`, so a stem-only name does not match.
 *
 * Observed behaviour this template cannot change:
 *   - a copied title and the real name are indistinguishable in the `name` field, so the template
 *     grades "copied" as "the title also matches";
 *   - a spaced query does not match an underscore name as exact.
 */
export function classOrderTemplateBody(): Record<string, unknown> {
  const title = ref(DISPLAY_TITLE, '.text');
  const ds = { term: { _entityType: 'dataset' } };
  const col = { term: { _entityType: 'schemaField' } };
  const exactName = { term: { 'name.lowercase': { value: '{{normalizedKeyword}}' } } };
  const phraseName = { match_phrase: { 'name.text': { query: '{{alphanumericSplit}}' } } };
  const titlePhrase = { match_phrase: { [title]: { query: '{{alphanumericSplit}}' } } };
  const partial = (field: string) => ({
    bool: {
      should: [
        { match: { [field]: { query: '{{alphanumericSplit}}' } } },
        { match_bool_prefix: { [field]: { query: '{{alphanumericSplit}}' } } },
      ],
    },
  });
  const tier = (boost: number, name: string, filter: unknown[], mustNot?: unknown[]) => ({
    constant_score: {
      boost,
      _name: `class:${name}`,
      filter: { bool: mustNot ? { filter, must_not: mustNot } : { filter } },
    },
  });
  const tiers = [
    // The stored ID keeps its case; compare the query as typed, ignoring case.
    tier(10000, 'IDENTIFIER', [ds, { term: { [ref(RECORD_ID, '')]: { value: '{{raw}}', case_insensitive: true } } }]),
  ];
  [
    { type: ds, tag: '', exact: [9000, 8000], partial: [5500, 5000, 4000] },
    { type: col, tag: 'COLUMN_', exact: [7000, 6000], partial: [2500, 2000, 1000] },
  ].forEach(({ type, tag, exact, partial: p }) => {
    tiers.push(
      tier(exact[0], `${tag}EXACT_NAME`, [type, exactName], [titlePhrase]),
      tier(exact[1], `${tag}EXACT_TITLE`, [type, exactName, titlePhrase]),
      tier(p[0], `${tag}PHRASE_NAME`, [type, phraseName], [titlePhrase]),
      tier(p[1], `${tag}PARTIAL_NAME`, [type, partial('name.text')], [partial(title)]),
      tier(p[2], `${tag}PARTIAL_TITLE`, [type, partial(title)]),
    );
  });
  return {
    bool: {
      minimum_should_match: 1,
      should: [
        { dis_max: { tie_breaker: 0.0, queries: tiers } },
        { match: { 'name.text': { query: '{{alphanumericSplit}}', boost: 0.001 } } },
      ],
    },
  };
}

export function classOrderTemplates(): { urn: string; searchTemplateProperties: { value: Record<string, unknown> } }[] {
  return [
    {
      urn: CLASS_ORDER_TEMPLATE_URN,
      searchTemplateProperties: {
        value: {
          name: 'Class order',
          description: 'Strict match-class order in the engine score.',
          dialect: 'ELASTICSEARCH',
          apiVersion: 1,
          editable: true,
          matchedFields: true,
          query: JSON.stringify(classOrderTemplateBody()),
          functionScore: JSON.stringify({
            functions: [{ weight: 1.0 }],
            score_mode: 'multiply',
            boost_mode: 'multiply',
          }),
          routes: [],
        },
      },
    },
    {
      urn: CLASS_ORDER_ROUTER_URN,
      searchTemplateProperties: {
        value: {
          name: 'Class order search',
          description: 'The shipped interactive router; ordinary queries use the class-order body.',
          dialect: 'ELASTICSEARCH',
          apiVersion: 1,
          editable: true,
          parent: 'urn:li:searchTemplate:searchLight',
          defaultRoute: { template: CLASS_ORDER_TEMPLATE_URN },
        },
      },
    },
  ];
}

// ── Seeding ──────────────────────────────────────────────────────────────────

const PROPERTIES = [
  {
    urn: DISPLAY_TITLE_URN,
    qualifiedName: DISPLAY_TITLE,
    displayName: 'Display title',
    entityTypes: ['dataset', 'schemaField'],
    searchConfiguration: { queryByDefault: true, copyTo: ['name'] },
  },
  {
    urn: RECORD_ID_URN,
    qualifiedName: RECORD_ID,
    displayName: 'Record ID',
    entityTypes: ['dataset'],
    searchConfiguration: { queryByDefault: true },
  },
];

export class SearchRankingSeeder {
  private readonly headers: Record<string, string>;

  constructor(
    private readonly request: APIRequestContext,
    private readonly gmsUrl: string,
    gmsToken: string,
  ) {
    this.headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${gmsToken}` };
  }

  private async graphql(query: string, variables: Record<string, unknown>): Promise<Record<string, unknown>> {
    const response = await this.request.post(`${this.gmsUrl}/api/graphql`, {
      headers: this.headers,
      data: { query, variables },
    });
    if (!response.ok()) throw new Error(`GraphQL ${response.status()}: ${(await response.text()).slice(0, 300)}`);
    return (await response.json()) as Record<string, unknown>;
  }

  private async upsert(entityName: string, entities: Record<string, unknown>[], chunk = 200): Promise<void> {
    for (let i = 0; i < entities.length; i += chunk) {
      const response = await this.request.post(
        `${this.gmsUrl}/openapi/v3/entity/${entityName}?async=false&createIfNotExists=false`,
        { headers: this.headers, data: entities.slice(i, i + chunk), timeout: 300_000 },
      );
      if (!response.ok()) {
        throw new Error(`upsert ${entityName} ${response.status()}: ${(await response.text()).slice(0, 300)}`);
      }
    }
  }

  /** Creates the two properties (GraphQL), then writes their search configuration (OpenAPI). */
  async ensureProperties(): Promise<void> {
    for (const p of PROPERTIES) {
      const entityTypes = p.entityTypes.map((t) => `urn:li:entityType:datahub.${t}`);
      // An existing property makes this mutation fail; the OpenAPI write below still applies.
      await this.graphql(
        'mutation create($input: CreateStructuredPropertyInput!) { createStructuredProperty(input: $input) { urn } }',
        {
          input: {
            id: p.qualifiedName,
            qualifiedName: p.qualifiedName,
            displayName: p.displayName,
            valueType: 'urn:li:dataType:datahub.string',
            cardinality: 'SINGLE',
            entityTypes,
          },
        },
      );
      await this.upsert('structuredProperty', [
        {
          urn: p.urn,
          propertyDefinition: {
            value: {
              qualifiedName: p.qualifiedName,
              displayName: p.displayName,
              valueType: 'urn:li:dataType:datahub.string',
              cardinality: 'SINGLE',
              entityTypes,
              searchConfiguration: p.searchConfiguration,
            },
          },
        },
      ]);
    }
  }

  async seedCorpus(corpus: CorpusEntry[]): Promise<void> {
    const properties = (e: CorpusEntry) => {
      const values = [
        ...(e.title ? [{ propertyUrn: DISPLAY_TITLE_URN, values: [{ string: e.title }] }] : []),
        ...(e.recordId ? [{ propertyUrn: RECORD_ID_URN, values: [{ string: e.recordId }] }] : []),
      ];
      return values.length ? { structuredProperties: { value: { properties: values } } } : {};
    };
    await this.upsert(
      'dataset',
      corpus
        .filter((e) => e.kind === 'dataset')
        .map((e) => ({
          urn: e.urn,
          datasetProperties: { value: { name: e.name, description: 'Search ranking corpus' } },
          ...properties(e),
        })),
    );
    await this.upsert(
      'schemaField',
      corpus
        .filter((e) => e.kind === 'column')
        .map((e) => {
          const parent = e.urn.slice('urn:li:schemaField:('.length, e.urn.lastIndexOf(','));
          return {
            urn: e.urn,
            schemaFieldKey: { value: { parent, fieldPath: e.name } },
            schemafieldInfo: { value: { name: e.name } },
            ...properties(e),
          };
        }),
    );
  }

  async seedTemplates(): Promise<void> {
    await this.upsert('searchTemplate', classOrderTemplates());
  }

  /** Total hits for a query through the default search path, used to wait for indexing. */
  async total(query: string): Promise<number> {
    const body = await this.graphql(
      `query s($q: String!) { searchAcrossEntities(input: { types: [DATASET], query: $q, start: 0, count: 0,
         searchFlags: { skipCache: true } }) { total } }`,
      { q: query },
    );
    return (body.data as { searchAcrossEntities?: { total?: number } } | undefined)?.searchAcrossEntities?.total ?? 0;
  }
}

/**
 * A copied title is searchable as text only after the index maps the property's `.text` sub-field
 * and its `copy_to`. A dataset found only through its title proves that.
 */
export async function rankingPropertiesSearchable(seeder: SearchRankingSeeder): Promise<boolean> {
  return (await seeder.total('extract')) > 0;
}

export async function ensureRankingCorpus(
  request: APIRequestContext,
  gmsUrl: string,
  gmsToken: string,
): Promise<CorpusEntry[]> {
  const corpus = buildRankingCorpus();
  const seeder = new SearchRankingSeeder(request, gmsUrl, gmsToken);
  await seeder.ensureProperties();
  await seeder.seedTemplates();
  // Upserts are idempotent, so a rerun also brings an older copy of the corpus up to date.
  await seeder.seedCorpus(corpus);
  // Every write is visible to search once the last entry of each kind is: wait for those.
  const lastDataset = corpus.filter((e) => e.kind === 'dataset').at(-1)!;
  const lastColumn = corpus.filter((e) => e.kind === 'column').at(-1)!;
  await waitUntilSearchable(request, gmsUrl, gmsToken, 'DATASET', lastDataset.urn, 120_000);
  await waitUntilSearchable(request, gmsUrl, gmsToken, 'SCHEMA_FIELD', lastColumn.urn, 120_000);
  return corpus;
}
