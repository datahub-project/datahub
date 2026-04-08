import { Page } from '@playwright/test';
import * as fs from 'fs/promises';
import { extractUrn, waitForSync, type Mcp, type Urn } from './seeder-utils';

/**
 * RestSeeder — seeds test data via the DataHub GMS REST API.
 *
 * Prefer seedingFixture for standard per-worker seeding. Use this class
 * directly only when a test needs fine-grained control over which entities
 * are created and when they are deleted.
 *
 * @example
 * ```typescript
 * const seeder = new RestSeeder(page, gmsToken);
 * const mcps = await seeder.loadTestData('./tests/search/fixtures/data.json');
 * const urns = await seeder.ingestMCPs(mcps);
 * await seeder.waitForSync(urns);
 * ```
 */
export class RestSeeder {
  private readonly gmsUrl: string;
  private readonly createdUrns: Urn[] = [];

  constructor(
    private readonly page: Page,
    private readonly gmsToken?: string,
  ) {
    const baseURL = process.env.BASE_URL ?? 'http://localhost:9002';
    this.gmsUrl = baseURL.replace(':9002', ':8080');
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

  /** Ingest an array of MCPs via the GMS `/entities?action=ingest` endpoint. */
  async ingestMCPs(mcps: Mcp[]): Promise<Urn[]> {
    const urns: Urn[] = [];

    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (this.gmsToken) headers['Authorization'] = `Bearer ${this.gmsToken}`;

    for (const mcp of mcps) {
      const urn = extractUrn(mcp);

      const response = await this.page.request.post(`${this.gmsUrl}/entities?action=ingest`, {
        data: { entity: { value: mcp.proposedSnapshot ?? mcp } },
        headers,
      });

      if (!response.ok()) {
        const body = await response.text();
        throw new Error(`Failed to ingest entity ${urn}: ${response.status()} ${body.slice(0, 200)}`);
      }

      urns.push(urn);
      this.createdUrns.push(urn);
    }

    return urns;
  }

  /** Wait until all URNs are reachable via the GMS REST API. */
  async waitForSync(urns: Urn[], timeout?: number): Promise<void> {
    await waitForSync(this.page.request, this.gmsUrl, urns, this.gmsToken, timeout);
  }

  getCreatedUrns(): Urn[] {
    return [...this.createdUrns];
  }

  clearCreatedUrns(): void {
    this.createdUrns.length = 0;
  }
}
