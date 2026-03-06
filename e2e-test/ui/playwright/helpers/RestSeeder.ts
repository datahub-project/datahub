import { Page } from '@playwright/test';
import * as fs from 'fs/promises';
import * as path from 'path';

/**
 * RestSeeder - Handles test data seeding via DataHub REST API
 *
 * Uses the DataHub ingest proposal endpoint to seed metadata from JSON files.
 * Tracks created URNs for cleanup after tests complete.
 *
 * @example
 * ```typescript
 * const seeder = new RestSeeder(page);
 * const mcps = await seeder.loadFixture('./fixtures/data.json');
 * const urns = await seeder.ingestMCPs(mcps);
 * await seeder.waitForSync(urns);
 * ```
 */
export class RestSeeder {
  private page: Page;
  private baseURL: string;
  private createdUrns: string[] = [];
  private gmsToken?: string;

  constructor(page: Page, gmsToken?: string) {
    this.page = page;
    this.baseURL = process.env.BASE_URL || 'http://localhost:9002';
    this.gmsToken = gmsToken;
  }

  /**
   * Load JSON fixture file containing MCPs (Metadata Change Proposals)
   *
   * @param relativePath - Path relative to the calling test file
   * @returns Array of MCP objects
   */
  async loadFixture(relativePath: string): Promise<any[]> {
    try {
      const content = await fs.readFile(relativePath, 'utf-8');
      const data = JSON.parse(content);

      if (!Array.isArray(data)) {
        throw new Error('Fixture data must be an array of MCPs');
      }

      console.log(`📦 Loaded ${data.length} entities from ${relativePath}`);
      return data;
    } catch (error) {
      throw new Error(`Failed to load fixture from ${relativePath}: ${error}`);
    }
  }

  /**
   * Ingest MCPs via DataHub REST API
   *
   * Uses the /entities?action=ingestProposal endpoint to create entities.
   * Tracks URNs for later cleanup.
   *
   * @param mcps - Array of Metadata Change Proposals
   * @returns Array of created entity URNs
   */
  async ingestMCPs(mcps: any[]): Promise<string[]> {
    const urns: string[] = [];
    const gmsUrl = this.baseURL.replace(':9002', ':8080');

    console.log(`🚀 Ingesting ${mcps.length} entities via ${gmsUrl}/entities?action=ingest`);

    for (const mcp of mcps) {
      try {
        const urn = this.extractUrn(mcp);

        const headers: Record<string, string> = {
          'Content-Type': 'application/json',
        };

        if (this.gmsToken) {
          headers['Authorization'] = `Bearer ${this.gmsToken}`;
        }

        const payload = {
          entity: {
            value: mcp.proposedSnapshot,
          },
        };

        const response = await this.page.request.post(`${gmsUrl}/entities?action=ingest`, {
          data: payload,
          headers,
        });

        if (!response.ok()) {
          const body = await response.text();
          throw new Error(
            `Failed to ingest entity ${urn}: ${response.status()} ${response.statusText()}\n${body}`
          );
        }

        urns.push(urn);
        this.createdUrns.push(urn);
      } catch (error) {
        console.error(`❌ Failed to ingest MCP:`, error);
        throw error;
      }
    }

    console.log(`✅ Successfully ingested ${urns.length} entities`);
    return urns;
  }

  /**
   * Extract URN from MCP
   *
   * Handles both snapshot format (proposedSnapshot) and MCP format (entityUrn)
   *
   * @param mcp - Metadata Change Proposal
   * @returns Entity URN
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
   *
   * Polls the search API until all entities are found or timeout occurs.
   * This ensures tests don't fail due to eventual consistency issues.
   *
   * @param urns - Entity URNs to wait for
   * @param timeout - Maximum wait time in milliseconds (default: 30s)
   */
  async waitForSync(urns: string[], timeout: number = 30000): Promise<void> {
    console.log(`⏳ Waiting for ${urns.length} entities to be indexed...`);

    const startTime = Date.now();
    const gmsUrl = this.baseURL.replace(':9002', ':8080');

    for (const urn of urns) {
      let found = false;

      while (!found && Date.now() - startTime < timeout) {
        try {
          // Check if entity exists via GET endpoint
          const headers: Record<string, string> = {};
          if (this.gmsToken) {
            headers['Authorization'] = `Bearer ${this.gmsToken}`;
          }

          const response = await this.page.request.get(`${gmsUrl}/entities/${encodeURIComponent(urn)}`, {
            headers,
            failOnStatusCode: false,
          });

          if (response.ok()) {
            found = true;
            continue;
          }
        } catch (error) {
          // Entity not found yet, continue waiting
        }

        // Wait 500ms before retrying
        await this.page.waitForTimeout(500);
      }

      if (!found) {
        throw new Error(`Timeout waiting for entity ${urn} to be indexed after ${timeout}ms`);
      }
    }

    // Additional buffer to ensure search index is updated
    await this.page.waitForTimeout(2000);

    console.log(`✅ All entities indexed and ready`);
  }

  /**
   * Get list of all URNs created during this session
   *
   * Used by cleanup helpers to delete test data
   *
   * @returns Array of entity URNs
   */
  getCreatedUrns(): string[] {
    return [...this.createdUrns];
  }

  /**
   * Clear the created URNs list
   *
   * Call this after cleanup to reset state
   */
  clearCreatedUrns(): void {
    this.createdUrns = [];
  }
}