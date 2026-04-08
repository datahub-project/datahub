import { Page } from '@playwright/test';
import * as fs from 'fs/promises';
import { extractUrn, waitForSync, type Mcp, type Urn } from './seeder-utils';

/**
 * GraphQLSeeder — seeds test data via the DataHub GraphQL API.
 *
 * Prefer seedingFixture for standard per-worker seeding. Use this class
 * directly only when a test needs fine-grained control over which entities
 * are created and when they are deleted.
 *
 * @example
 * ```typescript
 * const seeder = new GraphQLSeeder(page);
 * const mcps = await seeder.loadTestData('./tests/search/fixtures/data.json');
 * const urns = await seeder.ingestMCPs(mcps);
 * await seeder.waitForSync(urns);
 * ```
 */
export class GraphQLSeeder {
  private readonly baseURL: string;
  private readonly createdUrns: Urn[] = [];

  constructor(private readonly page: Page) {
    this.baseURL = process.env.BASE_URL ?? 'http://localhost:9002';
  }

  /** Load a JSON test-data file containing an array of MCPs. */
  async loadTestData(filePath: string): Promise<Mcp[]> {
    let content: string;
    try {
      content = await fs.readFile(filePath, 'utf-8');
    } catch (cause) {
      throw new Error(`Failed to read test data file: ${filePath}`, { cause });
    }

    const data: unknown = JSON.parse(content);
    if (!Array.isArray(data)) {
      throw new Error(`Test data file must contain a JSON array: ${filePath}`);
    }
    return data as Mcp[];
  }

  /** Ingest an array of MCPs via GraphQL soft-delete mutation trick. */
  async ingestMCPs(mcps: Mcp[]): Promise<Urn[]> {
    const urns: Urn[] = [];

    for (const mcp of mcps) {
      const urn = extractUrn(mcp);
      await this.ingestEntity(urn);
      urns.push(urn);
      this.createdUrns.push(urn);
    }

    return urns;
  }

  /** Wait until all URNs are reachable via the GMS REST API. */
  async waitForSync(urns: Urn[], timeout?: number): Promise<void> {
    const gmsUrl = this.baseURL.replace(':9002', ':8080');
    await waitForSync(this.page.request, gmsUrl, urns, undefined, timeout);
  }

  getCreatedUrns(): Urn[] {
    return [...this.createdUrns];
  }

  clearCreatedUrns(): void {
    this.createdUrns.length = 0;
  }

  // ── Private ─────────────────────────────────────────────────────────────────

  private async ingestEntity(urn: Urn): Promise<void> {
    // The batchUpdateSoftDeleted mutation with deleted:false is the lightest
    // GraphQL call that guarantees the entity exists in GMS.
    const mutation = `
      mutation updateEntity($input: BatchUpdateSoftDeletedInput!) {
        batchUpdateSoftDeleted(input: $input)
      }
    `;

    const response = await this.page.request.post(`${this.baseURL}/api/v2/graphql`, {
      data: { query: mutation, variables: { input: { urns: [urn], deleted: false } } },
    });

    if (!response.ok()) {
      const body = await response.text();
      throw new Error(`Failed to ingest entity ${urn}: ${response.status()} ${body.slice(0, 200)}`);
    }
  }
}
