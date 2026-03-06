import { Page } from '@playwright/test';
import * as fs from 'fs/promises';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as path from 'path';

const execAsync = promisify(exec);

/**
 * PythonSeeder - Handles test data seeding via Python ingestion script
 *
 * Uses the existing Cypress Python utilities to ingest data, ensuring
 * compatibility and reliability since Cypress tests already work.
 *
 * @example
 * ```typescript
 * const seeder = new PythonSeeder(page);
 * const urns = await seeder.ingestFixture('./fixtures/data.json');
 * await seeder.waitForSync(urns);
 * ```
 */
export class PythonSeeder {
  private page: Page;
  private baseURL: string;
  private createdUrns: string[] = [];

  constructor(page: Page) {
    this.page = page;
    this.baseURL = process.env.BASE_URL || 'http://localhost:9002';
  }

  /**
   * Ingest a fixture file via Python script
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

      // Extract URNs
      const urns = mcps.map((mcp) => this.extractUrn(mcp));

      // Use Python script to ingest
      const gmsUrl = this.baseURL.replace(':9002', ':8080');
      const smokeTestDir = path.resolve(__dirname, '../../../..');

      const pythonScript = `
import sys
sys.path.insert(0, '${smokeTestDir}/smoke-test')
from tests.utils import ingest_file_via_rest
from tests.utilities import env_vars
import requests

class SimpleSession:
    def gms_url(self):
        return '${gmsUrl}'

    def gms_token(self):
        # Use root token for ingestion
        return '__datahub_system'

session = SimpleSession()
ingest_file_via_rest(session, '${fixturePath}')
print('SUCCESS')
`;

      const tempScript = path.join('/tmp', `ingest_${Date.now()}.py`);
      await fs.writeFile(tempScript, pythonScript);

      console.log(`🚀 Ingesting via Python script...`);

      const { stdout, stderr } = await execAsync(`cd ${smokeTestDir}/smoke-test && python3 ${tempScript}`, {
        env: {
          ...process.env,
          DATAHUB_GMS_URL: gmsUrl,
        },
        maxBuffer: 10 * 1024 * 1024,
      });

      // Clean up temp script
      await fs.unlink(tempScript).catch(() => {});

      if (!stdout.includes('SUCCESS')) {
        throw new Error(`Python ingestion failed: ${stdout}\n${stderr}`);
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
    if (mcp.entityUrn) {
      return mcp.entityUrn;
    }

    if (mcp.proposedSnapshot) {
      const snapshot = Object.values(mcp.proposedSnapshot)[0] as any;
      if (snapshot?.urn) {
        return snapshot.urn;
      }
    }

    throw new Error('Unable to extract URN from MCP');
  }

  /**
   * Wait for entities to be indexed
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
            failOnStatusCode: false,
          });

          if (response.ok()) {
            found = true;
            continue;
          }
        } catch (error) {
          // Continue waiting
        }

        await this.page.waitForTimeout(500);
      }

      if (!found) {
        console.warn(`⚠️  Entity ${urn} not indexed after ${timeout}ms`);
      }
    }

    await this.page.waitForTimeout(2000);
    console.log(`✅ All entities indexed and ready`);
  }

  getCreatedUrns(): string[] {
    return [...this.createdUrns];
  }

  clearCreatedUrns(): void {
    this.createdUrns = [];
  }
}
