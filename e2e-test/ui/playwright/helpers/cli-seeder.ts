import { Page } from '@playwright/test';
import * as fs from 'fs/promises';
import { exec } from 'child_process';
import { promisify } from 'util';
import { extractUrn, waitForSync, type Mcp, type Urn } from './seeder-utils';

const execAsync = promisify(exec);

/**
 * CliSeeder — seeds test data via the DataHub CLI (`datahub ingest`).
 *
 * ## CLI dependency
 *
 * This class requires the `datahub` Python CLI to be installed and on `$PATH`.
 * Install it with the same version as the target DataHub instance:
 *
 *   pip install 'acryl-datahub==<version>'    # match the running DataHub version
 *
 * Check the installed version:
 *   datahub version
 *
 * The CLI authenticates via the `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN`
 * environment variables, or via the `server`/`token` fields in the generated
 * recipe passed to `datahub ingest -c`.
 *
 * Prefer seedingFixture (REST-based) for standard per-worker seeding — it has
 * no external CLI dependency. Use CliSeeder only when the REST API cannot
 * represent the entity aspects you need to ingest.
 *
 * @example
 * ```typescript
 * const seeder = new CliSeeder(page, gmsToken);
 * const urns = await seeder.ingestTestData('./tests/search/fixtures/data.json');
 * await seeder.waitForSync(urns);
 * ```
 */
export class CliSeeder {
  private readonly gmsUrl: string;
  private readonly createdUrns: Urn[] = [];

  constructor(
    private readonly page: Page,
    private readonly gmsToken: string,
  ) {
    const baseURL = process.env.BASE_URL ?? 'http://localhost:9002';
    this.gmsUrl = baseURL.replace(':9002', ':8080');
  }

  /**
   * Ingest a test-data file via the DataHub CLI.
   *
   * @param filePath - Absolute path to a JSON file containing an MCP array.
   * @returns Array of ingested entity URNs.
   */
  async ingestTestData(filePath: string): Promise<Urn[]> {
    const content = await fs.readFile(filePath, 'utf-8');
    const mcps: unknown = JSON.parse(content);

    if (!Array.isArray(mcps)) {
      throw new Error(`Test data file must contain a JSON array: ${filePath}`);
    }

    const urns = (mcps as Mcp[]).map(extractUrn);

    const recipe = JSON.stringify({
      source: { type: 'file', config: { filename: filePath } },
      sink: { type: 'datahub-rest', config: { server: this.gmsUrl, token: this.gmsToken } },
    });

    const { stderr } = await execAsync(`datahub ingest -c '${recipe}'`, {
      env: {
        ...process.env,
        DATAHUB_GMS_URL: this.gmsUrl,
        DATAHUB_GMS_TOKEN: this.gmsToken,
      },
      maxBuffer: 10 * 1024 * 1024,
    });

    // The CLI prints WARNING lines for non-fatal issues; only surface real errors.
    if (stderr && !stderr.includes('WARNING')) {
      throw new Error(`datahub ingest produced unexpected stderr:\n${stderr.slice(0, 500)}`);
    }

    this.createdUrns.push(...urns);
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
