/**
 * URN-values seeder: one multi-valued string structured property, shown in the asset summary
 * sidebar, whose values on a single glossary term are the URNs of N other glossary terms. This is
 * a common shape in the wild: a plain string property (no allowed-values constraint) holding
 * thousands of values that each name an entity, so every value gets hydrated on the page.
 *
 * Everything is derived from the option set, so a given `UrnValuesOptions` yields the same
 * property, value terms and carrier term on every run. Seeding goes through the OpenAPI v3 entity
 * endpoint in chunks. Re-running is cheap: `ensureUrnValuesFixture` checks the server first and only
 * seeds when the carrier term is missing or has a different number of values.
 *
 * Usage from a spec:
 *   const fixture = await ensureUrnValuesFixture(request, gmsUrl(), gmsToken, { values: 5000 });
 *   await page.goto(`/glossaryTerm/${encodeURIComponent(fixture.termUrn)}`);
 */
import type { APIRequestContext } from '@playwright/test';
import { waitUntilSearchable } from './search-wait';

export interface UrnValuesOptions {
  /** Number of glossary terms referenced by the property, one URN value each. */
  values: number;
}

export interface UrnValuesFixture {
  values: number;
  propertyUrn: string;
  qualifiedName: string;
  displayName: string;
  /** The glossary node that holds the referenced terms. */
  nodeUrn: string;
  /** Every referenced term, in value order. */
  valueTerms: { urn: string; name: string }[];
  /** The term that carries the property. */
  termUrn: string;
  termName: string;
}

const NAMESPACE = 'scale_smoke';

export function buildUrnValuesFixture(options: UrnValuesOptions): UrnValuesFixture {
  const { values } = options;
  if (!Number.isInteger(values) || values < 1) {
    throw new Error(`urn values fixture: values must be a positive integer (got ${values})`);
  }
  const width = String(values).length;
  const qualifiedName = `${NAMESPACE}.referenced_terms_${values}`;
  const termName = `${NAMESPACE}_referenced_terms_${values}_term`;
  const nodeUrn = `urn:li:glossaryNode:${NAMESPACE}_referenced_terms_${values}_pool`;
  const valueTerms = Array.from({ length: values }, (_, i) => {
    const name = `${NAMESPACE}_referenced_term_${values}_${String(i).padStart(width, '0')}`;
    return { urn: `urn:li:glossaryTerm:${name}`, name };
  });
  return {
    values,
    propertyUrn: `urn:li:structuredProperty:${qualifiedName}`,
    qualifiedName,
    displayName: `Scale Referenced Terms (${values})`,
    nodeUrn,
    valueTerms,
    termUrn: `urn:li:glossaryTerm:${termName}`,
    termName,
  };
}

type V3Entity = { urn: string } & Record<string, unknown>;

export class UrnValuesSeeder {
  private readonly headers: Record<string, string>;

  constructor(
    private readonly request: APIRequestContext,
    private readonly gmsUrl: string,
    private readonly gmsToken: string,
  ) {
    this.headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${gmsToken}` };
  }

  /** True when the carrier term already holds this many values of the property. */
  async alreadySeeded(fixture: UrnValuesFixture): Promise<boolean> {
    const term = await this.request.get(
      `${this.gmsUrl}/openapi/v3/entity/glossaryTerm/${encodeURIComponent(fixture.termUrn)}?aspects=structuredProperties`,
      { headers: this.headers, failOnStatusCode: false },
    );
    if (!term.ok()) return false;
    const body = (await term.json()) as {
      structuredProperties?: { value?: { properties?: { propertyUrn: string; values: unknown[] }[] } };
    };
    const entry = body.structuredProperties?.value?.properties?.find((p) => p.propertyUrn === fixture.propertyUrn);
    return entry?.values.length === fixture.values;
  }

  async seed(fixture: UrnValuesFixture): Promise<void> {
    await this.upsert('structuredProperty', [
      {
        urn: fixture.propertyUrn,
        propertyDefinition: {
          value: {
            qualifiedName: fixture.qualifiedName,
            displayName: fixture.displayName,
            description: `Scale smoke property holding ${fixture.values} glossary term URNs as string values`,
            valueType: 'urn:li:dataType:datahub.string',
            cardinality: 'MULTIPLE',
            entityTypes: ['urn:li:entityType:datahub.glossaryTerm'],
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
      },
    ]);

    await this.upsert('glossaryNode', [
      {
        urn: fixture.nodeUrn,
        glossaryNodeInfo: {
          value: {
            name: `Scale referenced term pool (${fixture.values})`,
            definition: 'Referenced by the scale smoke term',
          },
        },
      },
    ]);

    await this.upsert(
      'glossaryTerm',
      fixture.valueTerms.map((t) => ({
        urn: t.urn,
        glossaryTermInfo: {
          value: {
            name: t.name,
            definition: `Scale smoke value term ${t.name}`,
            termSource: 'INTERNAL',
            parentNode: fixture.nodeUrn,
          },
        },
      })),
      250,
    );

    await this.upsert('glossaryTerm', [
      {
        urn: fixture.termUrn,
        glossaryTermInfo: {
          value: {
            name: fixture.termName,
            definition: `Scale smoke glossary term whose property references ${fixture.values} other terms`,
            termSource: 'INTERNAL',
          },
        },
        structuredProperties: {
          value: {
            properties: [
              { propertyUrn: fixture.propertyUrn, values: fixture.valueTerms.map((t) => ({ string: t.urn })) },
            ],
          },
        },
      },
    ]);

    // The sidebar and Properties tab discover properties through search; wait for the index.
    await waitUntilSearchable(this.request, this.gmsUrl, this.gmsToken, 'STRUCTURED_PROPERTY', fixture.propertyUrn);
  }

  private async upsert(entityName: string, entities: V3Entity[], chunkSize = entities.length): Promise<void> {
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
 * Build the fixture description and make sure it exists on the server. Set `URN_VALUES_RESEED=1`
 * to force a re-seed even when the server already has it.
 */
export async function ensureUrnValuesFixture(
  request: APIRequestContext,
  gmsUrl: string,
  gmsToken: string,
  options: UrnValuesOptions,
): Promise<{ fixture: UrnValuesFixture; seeded: boolean; seedMs: number }> {
  const fixture = buildUrnValuesFixture(options);
  const seeder = new UrnValuesSeeder(request, gmsUrl, gmsToken);
  if (process.env.URN_VALUES_RESEED !== '1' && (await seeder.alreadySeeded(fixture))) {
    return { fixture, seeded: false, seedMs: 0 };
  }
  const started = Date.now();
  await seeder.seed(fixture);
  return { fixture, seeded: true, seedMs: Date.now() - started };
}
