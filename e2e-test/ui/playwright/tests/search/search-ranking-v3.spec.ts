/**
 * Search ranking regression on Search V3, through the search box and the results page.
 *
 * A pet-shop catalog (helpers/seeders/search-ranking-seeder.ts) holds the shapes that break
 * ranking at scale: many datasets whose display title (a structured property copied into the
 * name) equals one dataset's real name, stem-only names, a column named like a dataset, and
 * record identifiers, plus near misses that hold the query words without being the whole value.
 * Every test types the query as a user does, without quotes, reads the results in order and checks
 * them against the tranching rule, graded from the corpus definition:
 *
 *   EXACT_NAME > EXACT_COPIED > EXACT_COLUMN > EXACT_COLUMN_COPIED >
 *   PARTIAL_NAME > PARTIAL_COPIED > PARTIAL_COLUMN > PARTIAL_COLUMN_COPIED > OTHER
 *
 * Exact is a whole-value match ignoring case; partial is every separator-delimited substring of the
 * query starting one of the value (see `tranche` in the seeder). A value that differs from the query only in separators is
 * left out of the comparison. A tranche holds ties in any order; a result in the wrong tranche
 * fails the test, and the failure message shows the expected and the actual order. Each search attaches its served rows, their
 * tranches and the first results page to the test (attachments `ranking-<n>.json` and
 * `ranking-<n>.png`), so a report can show exactly what the assertion saw.
 *
 * Requirements:
 *   - Search V3 keyword read and templates on (ELASTICSEARCH_ENTITY_INDEX_V3_ENABLED,
 *     ELASTICSEARCH_ENTITY_INDEX_V3_KEYWORD_READ_ENABLED, SEARCH_V3_TEMPLATES_ENABLED).
 *   - SEARCH_RANKING_MODE names what the search box runs (it labels the run; the assertions are the
 *     same):
 *       stock       the shipped templates;
 *       classOrder  the seeded class-order router, selected the way a deployment selects a
 *                   default template, by pointing the search surface at it in GMS configuration:
 *                   SPRING_APPLICATION_JSON='{"elasticsearch":{"search":{"v3":{"templates":
 *                   {"surfaces":{"searchEntities":{"fulltext":{"default":
 *                   "urn:li:searchTemplate:rankingClassOrderSearch"}}}}}}}}'
 *   - The two structured properties must exist before the Search V3 index is built, or a
 *     system-update must run after the first seeding: a property created later is mapped as a
 *     keyword only, so its copied title is not searchable as text. The suite skips with that reason.
 */
import type { TestInfo } from '@playwright/test';
import { test, expect } from '../../fixtures/base-test';
import { SearchRankingPage } from '../../pages/search-ranking.page';
import { gmsUrl } from '../../utils/constants';
import { DomainTag } from '../../utils/test-tags';
import {
  CorpusEntry,
  GUID,
  SearchRankingSeeder,
  Tranche,
  buildRankingCorpus,
  datasetUrn,
  ensureRankingCorpus,
  rankingPropertiesSearchable,
  tranche,
  trancheRank,
} from '../../helpers/seeders/search-ranking-seeder';

const MODE = process.env.SEARCH_RANKING_MODE;

const corpus = buildRankingCorpus();
const byUrn = new Map(corpus.map((e) => [e.urn, e]));

/** Entries the rule places: every match except values that differ from the query only in separators. */
const graded = (query: string): CorpusEntry[] => corpus.filter((e) => tranche(query, e) !== 'OTHER');

/** The corpus entries the query should return in rule order, best tranche first. */
function expectedOrder(query: string): CorpusEntry[] {
  return graded(query)
    .filter((e) => tranche(query, e) !== 'SEPARATOR_ONLY')
    .sort((a, b) => trancheRank(tranche(query, a)) - trancheRank(tranche(query, b)));
}

const trancheOf = (query: string, e: CorpusEntry | undefined): Tranche | 'OUTSIDE' =>
  e ? tranche(query, e) : 'OUTSIDE';

/** The first three names, then runs of one tranche, e.g. "EXACT_COPIED x150 > EXACT_NAME x1". */
function summarize(query: string, entries: (CorpusEntry | undefined)[]): string {
  const head = entries
    .slice(0, 3)
    .map((e, i) => `#${i + 1} ${e?.name ?? 'outside the corpus'}`)
    .join(', ');
  const runs: string[] = [];
  let run: string | undefined;
  let count = 0;
  for (const e of entries) {
    const t = trancheOf(query, e);
    if (t === run) {
      count += 1;
    } else {
      if (run) runs.push(`${run} x${count}`);
      run = t;
      count = 1;
    }
  }
  if (run) runs.push(`${run} x${count}`);
  return `${head} | ${runs.join(' > ')}`;
}

const rowOf = (query: string, rank: number, urn: string, e: CorpusEntry | undefined) => ({
  rank,
  urn,
  name: e?.name ?? urn,
  copiedTitle: e?.title ?? '',
  type: e?.kind ?? '',
  tranche: trancheOf(query, e),
});

test.describe('Search ranking on Search V3', { tag: [DomainTag.Catalog] }, () => {
  test.setTimeout(5 * 60_000);
  test.skip(!MODE, 'Set SEARCH_RANKING_MODE=stock or classOrder on a stack with Search V3 keyword read on');

  let rankingPage: SearchRankingPage;

  test.beforeAll(async ({ playwright, gmsToken }) => {
    test.setTimeout(10 * 60_000);
    const request = await playwright.request.newContext();
    try {
      await ensureRankingCorpus(request, gmsUrl(), gmsToken);
      const searchable = await rankingPropertiesSearchable(new SearchRankingSeeder(request, gmsUrl(), gmsToken));
      test.skip(!searchable, 'Copied titles are not searchable as text yet: run a system-update after seeding');
    } finally {
      await request.dispose();
    }
  });

  test.beforeEach(async ({ page, logger, logDir }) => {
    rankingPage = new SearchRankingPage(page, logger, logDir);
  });

  /**
   * Searches, attaches what was served (rows, tranches, expected order, first-page screenshot) and
   * returns the served entries in order.
   */
  async function searchAndRecord(
    testInfo: TestInfo,
    query: string,
    depth: number,
  ): Promise<{ actual: (CorpusEntry | undefined)[]; expected: CorpusEntry[]; urns: string[]; totals: number[] }> {
    const expected = expectedOrder(query);
    const { urns, totals, firstPage } = await rankingPage.readResults(query, depth);
    const actual = urns.map((urn) => byUrn.get(urn));
    const n = testInfo.attachments.filter((a) => a.name.endsWith('.json')).length + 1;
    await testInfo.attach(`ranking-${n}.json`, {
      contentType: 'application/json',
      body: JSON.stringify({
        mode: MODE,
        query,
        totals,
        served: urns.map((urn, i) => rowOf(query, i + 1, urn, byUrn.get(urn))),
        expected: expected.map((e, i) => rowOf(query, i + 1, e.urn, e)),
      }),
    });
    await testInfo.attach(`ranking-${n}.png`, { contentType: 'image/png', body: firstPage });
    return { actual, expected, urns, totals };
  }

  /** Reads every match the rule places and checks the served order against it, tranche by tranche. */
  async function expectTrancheOrder(testInfo: TestInfo, query: string) {
    const { actual, expected } = await searchAndRecord(testInfo, query, graded(query).length);
    const ranked = actual.filter((e) => trancheOf(query, e) !== 'SEPARATOR_ONLY').slice(0, expected.length);
    expect(
      ranked.map((e) => trancheOf(query, e)),
      `query ${query} out of tranche order\n  expected: ${summarize(query, expected)}\n  actual:   ${summarize(query, actual)}`,
    ).toEqual(expected.map((e) => tranche(query, e)));
    return actual;
  }

  test('my cat: the exact name, then the exact title, then the partials, then the near misses', async ({}, testInfo) => {
    // "my cat frank" and "awesome my cat" are both partial; their order between themselves is not specified.
    const actual = await expectTrancheOrder(testInfo, 'my cat');
    expect(actual[0]?.name).toBe('my cat');
  });

  test('only the whole name is an exact match, and near misses rank below it', async ({}, testInfo) => {
    // kennel_booking_archive, kennel_booking_2024, booking_kennel, kennel, booking, the title
    // "Kennel Booking Archive" and the column kennel_booking_id all hold the words; none is exact.
    // 150 datasets titled "Kennel Booking" differ only in separators and are left out of the check.
    const actual = await expectTrancheOrder(testInfo, 'kennel_booking');
    expect(actual[0]?.urn).toBe(datasetUrn('kennel_booking'));
  });

  test('the quoted query gives the same order as the unquoted one', async ({}, testInfo) => {
    const actual = await expectTrancheOrder(testInfo, '"kennel_booking"');
    expect(actual[0]?.urn).toBe(datasetUrn('kennel_booking'));
  });

  test('only the whole title is an exact copied match', async ({}, testInfo) => {
    // groomed_journal is titled "Groomed Journal Entries"; 20 datasets carry "Groomed Journal Entries N".
    const actual = await expectTrancheOrder(testInfo, 'groomed journal entries');
    expect(actual[0]?.name).toBe('groomed_journal');
  });

  test('a name sharing only a stem ranks below an exact copied title', async ({}, testInfo) => {
    // gcn_ref is titled "Groomed Coat Notes"; groom_coat_notes_stage shares only the stem of "groomed".
    await expectTrancheOrder(testInfo, 'groomed coat notes');
  });

  test('a real name equal to its own title ranks as the real name', async ({}, testInfo) => {
    const actual = await expectTrancheOrder(testInfo, 'cattery roster');
    expect(actual[0]?.name).toBe('Cattery Roster');
  });

  test('partial real names come before partial titles, and stem-only names after every real match', async ({}, testInfo) => {
    // groomed_journal also carries the word in its own title; it is still a partial real-name match.
    const actual = await expectTrancheOrder(testInfo, 'groomed');
    expect(
      actual
        .slice(0, 3)
        .map((e) => e?.name)
        .sort(),
    ).toEqual(['groomed_coat_notes', 'groomed_journal', 'salon_audit_groomed']);
  });

  test('a dataset ranks above a column with the same name', async ({}, testInfo) => {
    const actual = await expectTrancheOrder(testInfo, 'litter_tray');
    expect(actual.slice(0, 2).map((e) => e?.kind)).toEqual(['dataset', 'column']);
  });

  test('an exact name with a longer title of its own stays on the first page', async ({}, testInfo) => {
    // 12 datasets titled exactly the query live in a schema named like the table family; the real
    // name's own title is longer. The control's real name has no title.
    for (const query of ['ragdoll lounge', 'siberian lounge']) {
      const { actual } = await searchAndRecord(testInfo, query, 10);
      expect(
        actual[0]?.name,
        `query ${query}: the exact real name should be #1 on the first page\n  actual:   ${summarize(query, actual)}`,
      ).toBe(query);
    }
  });

  test('a record identifier finds its dataset first, as typed and in upper case', async ({}, testInfo) => {
    for (const query of [GUID, GUID.toUpperCase()]) {
      const { urns } = await searchAndRecord(testInfo, query, 1);
      expect(urns[0], `query ${query}`).toBe(datasetUrn('adoption_register'));
    }
  });

  test('paging shows every match once with a stable total', async ({}, testInfo) => {
    const query = 'kennel_booking';
    const matches = graded(query);
    const { urns, totals } = await searchAndRecord(testInfo, query, matches.length + 10);
    expect(new Set(urns).size, 'a result appeared on two pages').toBe(urns.length);
    expect(new Set(totals).size, `totals changed between pages: ${totals.join(', ')}`).toBe(1);
    expect(new Set(urns.filter((urn) => matches.some((e) => e.urn === urn)))).toEqual(
      new Set(matches.map((e) => e.urn)),
    );
  });

  test('name shapes: many-part names, accents and leading digits', async ({}, testInfo) => {
    const multipart = await searchAndRecord(testInfo, 'A_B_C_D_E_F_G_H', 1);
    expect(multipart.urns[0]).toBe(datasetUrn('a_b_c_d_e_f_g_h'));
    const accented = await searchAndRecord(testInfo, 'cafe grooming orders', 1);
    expect(accented.urns[0]).toBe(datasetUrn('Café Grooming Orders'));
    const digits = await searchAndRecord(testInfo, '2024 kennel', 1);
    expect(digits.urns[0]).toBe(datasetUrn('2024_kennel_budget'));
  });
});
