/**
 * Allowed-values seeder: one string structured property whose definition carries a large
 * list of allowed values, applicable to glossary terms, plus one glossary term that has the
 * property set to one of those values.
 *
 * Everything is derived from the option set, so a given `AllowedValuesOptions` yields the same
 * property, values and term on every run, and the spec can assert on specific values. Seeding
 * goes through the OpenAPI v3 entity endpoint. Re-running is cheap: `ensureAllowedValuesFixture`
 * checks the server first and only seeds when the definition is missing or has a different size.
 *
 * Usage from a spec:
 *   const fixture = await ensureAllowedValuesFixture(request, gmsUrl(), gmsToken, { values: 5000 });
 *   await page.goto(`/glossaryTerm/${encodeURIComponent(fixture.termUrn)}`);
 */
import type { APIRequestContext } from '@playwright/test';
import { waitUntilSearchable } from './search-wait';

export interface AllowedValuesOptions {
  /** Number of allowed values on the property definition. */
  values: number;
}

export interface AllowedValuesFixture {
  values: number;
  propertyUrn: string;
  qualifiedName: string;
  displayName: string;
  /** Every allowed value, in definition order. */
  allowedValues: string[];
  /** The value assigned to the term (from the middle of the list, so it is not trivially first). */
  assignedValue: string;
  termUrn: string;
  termName: string;
}

const NAMESPACE = 'scale_smoke';

/** Zero-padded so the values sort and filter predictably in the UI. */
export const allowedValue = (index: number, total: number): string =>
  `choice_${String(index).padStart(String(total).length, '0')}`;

/** The value count is part of every identifier, so differently-sized fixtures never collide. */
export function buildAllowedValuesFixture(options: AllowedValuesOptions): AllowedValuesFixture {
  const { values } = options;
  if (!Number.isInteger(values) || values < 1) {
    throw new Error(`allowed values fixture: values must be a positive integer (got ${values})`);
  }
  const qualifiedName = `${NAMESPACE}.allowed_values_${values}`;
  const termName = `${NAMESPACE}_allowed_values_${values}_term`;
  const allowedValues = Array.from({ length: values }, (_, i) => allowedValue(i, values));
  return {
    values,
    propertyUrn: `urn:li:structuredProperty:${qualifiedName}`,
    qualifiedName,
    displayName: `Scale Allowed Values (${values})`,
    allowedValues,
    assignedValue: allowedValues[Math.floor(values / 2)],
    termUrn: `urn:li:glossaryTerm:${termName}`,
    termName,
  };
}

type V3Entity = { urn: string } & Record<string, unknown>;

export class AllowedValuesSeeder {
  private readonly headers: Record<string, string>;

  constructor(
    private readonly request: APIRequestContext,
    private readonly gmsUrl: string,
    private readonly gmsToken: string,
  ) {
    this.headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${gmsToken}` };
  }

  /** True when the property definition has this many allowed values and the term carries the property. */
  async alreadySeeded(fixture: AllowedValuesFixture): Promise<boolean> {
    const property = await this.request.get(
      `${this.gmsUrl}/openapi/v3/entity/structuredProperty/${encodeURIComponent(fixture.propertyUrn)}?aspects=propertyDefinition`,
      { headers: this.headers, failOnStatusCode: false },
    );
    if (!property.ok()) return false;
    const body = (await property.json()) as { propertyDefinition?: { value?: { allowedValues?: unknown[] } } };
    if (body.propertyDefinition?.value?.allowedValues?.length !== fixture.values) return false;

    const term = await this.request.get(
      `${this.gmsUrl}/openapi/v3/entity/glossaryTerm/${encodeURIComponent(fixture.termUrn)}?aspects=structuredProperties`,
      { headers: this.headers, failOnStatusCode: false },
    );
    if (!term.ok()) return false;
    const termBody = (await term.json()) as {
      structuredProperties?: { value?: { properties?: { propertyUrn: string }[] } };
    };
    return (
      termBody.structuredProperties?.value?.properties?.some((p) => p.propertyUrn === fixture.propertyUrn) ?? false
    );
  }

  async seed(fixture: AllowedValuesFixture): Promise<void> {
    await this.upsert('structuredProperty', [
      {
        urn: fixture.propertyUrn,
        propertyDefinition: {
          value: {
            qualifiedName: fixture.qualifiedName,
            displayName: fixture.displayName,
            description: `Scale smoke property with ${fixture.values} allowed values`,
            valueType: 'urn:li:dataType:datahub.string',
            cardinality: 'SINGLE',
            entityTypes: ['urn:li:entityType:datahub.glossaryTerm'],
            allowedValues: fixture.allowedValues.map((value) => ({
              value: { string: value },
              description: `Allowed value ${value}`,
            })),
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

    await this.upsert('glossaryTerm', [
      {
        urn: fixture.termUrn,
        glossaryTermInfo: {
          value: {
            name: fixture.termName,
            definition: `Scale smoke glossary term carrying a property with ${fixture.values} allowed values`,
            termSource: 'INTERNAL',
          },
        },
        structuredProperties: {
          value: {
            properties: [{ propertyUrn: fixture.propertyUrn, values: [{ string: fixture.assignedValue }] }],
          },
        },
      },
    ]);

    // The sidebar and Properties tab discover properties through search; wait for the index.
    await waitUntilSearchable(this.request, this.gmsUrl, this.gmsToken, 'STRUCTURED_PROPERTY', fixture.propertyUrn);
  }

  private async upsert(entityName: string, entities: V3Entity[]): Promise<void> {
    const response = await this.request.post(`${this.gmsUrl}/openapi/v3/entity/${entityName}?async=false`, {
      data: entities,
      headers: this.headers,
      failOnStatusCode: false,
      timeout: 120_000,
    });
    if (!response.ok()) {
      const body = await response.text();
      throw new Error(`scale seed: upsert ${entityName} failed: ${response.status()} ${body.slice(0, 400)}`);
    }
  }
}

/**
 * Build the fixture description and make sure it exists on the server. Set
 * `ALLOWED_VALUES_RESEED=1` to force a re-seed even when the server already has it.
 */
export async function ensureAllowedValuesFixture(
  request: APIRequestContext,
  gmsUrl: string,
  gmsToken: string,
  options: AllowedValuesOptions,
): Promise<{ fixture: AllowedValuesFixture; seeded: boolean; seedMs: number }> {
  const fixture = buildAllowedValuesFixture(options);
  const seeder = new AllowedValuesSeeder(request, gmsUrl, gmsToken);
  if (process.env.ALLOWED_VALUES_RESEED !== '1' && (await seeder.alreadySeeded(fixture))) {
    return { fixture, seeded: false, seedMs: 0 };
  }
  const started = Date.now();
  await seeder.seed(fixture);
  return { fixture, seeded: true, seedMs: Date.now() - started };
}
