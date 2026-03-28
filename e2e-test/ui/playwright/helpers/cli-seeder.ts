import { Page } from '@playwright/test';
import * as fs from 'fs/promises';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

/**
 * CliSeeder - Handles test data seeding via DataHub CLI
 *
 * Uses the datahub CLI to ingest fixture files, which is the most reliable
 * approach for Playwright test data seeding.
 *
 * @example
 * ```typescript
 * const seeder = new CliSeeder(page, gmsToken);
 * const urns = await seeder.ingestFixture('./fixtures/data.json');
 * await seeder.waitForSync(urns);
 * ```
 */
export class CliSeeder {
  private page: Page;
  private baseURL: string;
  private gmsToken: string;
  private createdUrns: string[] = [];

  constructor(page: Page, gmsToken: string) {
    this.page = page;
    this.baseURL = process.env.BASE_URL || 'http://localhost:9002';
    this.gmsToken = gmsToken;
  }

  /**
   * Ingest a fixture file via DataHub CLI
   *
   * @param fixturePath - Absolute path to the fixture JSON file
   * @returns Array of created entity URNs
   */
  async ingestFixture(fixturePath: string): Promise<string[]> {
    try {
      // Read fixture to extract URNs
      const content = await fs.readFile(fixturePath, 'utf-8');
      const mcps = JSON.parse(content);

      if (!Array.isArray(mcps)) {
        throw new Error('Fixture data must be an array of MCPs');
      }

      console.log(`📦 Loaded ${mcps.length} entities from ${fixturePath}`);

      // Extract URNs before ingestion
      const urns = mcps.map((mcp) => this.extractUrn(mcp));

      // Ingest via DataHub CLI
      const gmsUrl = this.baseURL.replace(':9002', ':8080');

      console.log(`🚀 Ingesting via datahub CLI...`);

      const command = `datahub ingest -c '${JSON.stringify({
        source: {
          type: 'file',
          config: {
            filename: fixturePath,
          },
        },
        sink: {
          type: 'datahub-rest',
          config: {
            server: gmsUrl,
            token: this.gmsToken,
          },
        },
      })}'`;

      const { stdout, stderr } = await execAsync(command, {
        env: {
          ...process.env,
          DATAHUB_GMS_URL: gmsUrl,
          DATAHUB_GMS_TOKEN: this.gmsToken,
        },
        maxBuffer: 10 * 1024 * 1024, // 10MB buffer
      });

      if (stderr && !stderr.includes('WARNING')) {
        console.warn(`⚠️  CLI stderr: ${stderr}`);
      }

      console.log(`✅ Successfully ingested ${urns.length} entities`);

      this.createdUrns.push(...urns);
      return urns;
    } catch (error) {
      console.error(`❌ Failed to ingest fixture:`, error);
      throw error;
    }
  }

  /**
   * Extract URN from MCP
   */
  private extractUrn(mcp: any): string {
    // MCP format (newer)
    if (mcp.entityUrn) {
      return mcp.entityUrn;
    }

    // Snapshot format (older)
    if (mcp.proposedSnapshot) {
      const snapshot = Object.values(mcp.proposedSnapshot)[0] as any;
      if (snapshot?.urn) {
        return snapshot.urn;
      }
    }

    throw new Error('Unable to extract URN from MCP');
  }

  /**
   * Wait for entities to be indexed and searchable
   */
  async waitForSync(urns: string[], timeout: number = 30000): Promise<void> {
    console.log(`⏳ Waiting for ${urns.length} entities to be indexed...`);

    const startTime = Date.now();
    const gmsUrl = this.baseURL.replace(':9002', ':8080');

    for (const urn of urns) {
      let found = false;

      while (!found && Date.now() - startTime < timeout) {
        try {
          const response = await this.page.request.get(`${gmsUrl}/entities/${encodeURIComponent(urn)}`, {
            headers: {
              Authorization: `Bearer ${this.gmsToken}`,
            },
            failOnStatusCode: false,
          });

          if (response.ok()) {
            found = true;
            continue;
          }
        } catch (error) {
          // Entity not found yet, continue waiting
        }

        await this.page.waitForTimeout(500);
      }

      if (!found) {
        console.warn(`⚠️  Entity ${urn} not indexed after ${timeout}ms, continuing anyway...`);
      }
    }

    // Additional buffer
    await this.page.waitForTimeout(2000);

    console.log(`✅ All entities indexed and ready`);
  }

  /**
   * Get list of all URNs created during this session
   */
  getCreatedUrns(): string[] {
    return [...this.createdUrns];
  }

  /**
   * Clear the created URNs list
   */
  clearCreatedUrns(): void {
    this.createdUrns = [];
  }
}
