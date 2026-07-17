/**
 * Test data loading capability.
 *
 * Provides a `FeatureDataLoader` that:
 *  - Reads `{featureDir}/data/{featureName}.json`
 *  - Checks whether entities with the feature prefix already exist in DataHub
 *    (idempotency — safe for long-running environments)
 *  - Ingests MCPs via the DataHub REST API using the GMS API token
 *  - Respects the `PW_NO_SETUP=1` env flag (bypass for pre-seeded local stacks)
 *
 * The `featureData` fixture is provided by base-test.ts.
 * Import `FeatureDataLoader` here when you need the type in setup helpers.
 *
 * Conventions enforced here:
 *   - Data file location : {featureDir}/data/{featureName}.json   (§6)
 *   - Entity prefix      : pw_{featureName}_                       (§6)
 *   - Injection skipped  : if prefix exists OR PW_NO_SETUP=1      (§6)
 */

import * as fs from 'fs';
import * as path from 'path';
import type { APIRequestContext } from '@playwright/test';
import { extractUrn, type Mcp } from '../helpers/seeder-utils';
import { createScriptLogger, type DataHubLogger } from './logger';

// ── Public interface ──────────────────────────────────────────────────────────

export interface FeatureDataLoader {
  /**
   * Inject the feature's data file into DataHub if not already present.
   *
   * @param featureDir  - Absolute path to the feature's test directory
   *                      (i.e. `__dirname` inside a spec file)
   * @param featureName - Feature identifier, must match the JSON filename and
   *                      be the prefix root (e.g. `'search'` → `pw_search_`)
   */
  load(featureDir: string, featureName: string): Promise<void>;
}

async function prefixExists(
  request: APIRequestContext,
  gmsUrl: string,
  prefix: string,
  gmsToken: string,
): Promise<boolean> {
  const response = await request.post(`${gmsUrl}/api/v2/graphql`, {
    data: {
      query: `query search($input: SearchInput!) { search(input: $input) { total } }`,
      variables: { input: { type: 'DATASET', query: `${prefix}*`, start: 0, count: 1 } },
    },
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${gmsToken}`,
    },
  });
  const json = (await response.json()) as { data?: { search?: { total?: number } } };
  return (json.data?.search?.total ?? 0) > 0;
}

export class RestFeatureDataLoader implements FeatureDataLoader {
  private readonly logger: DataHubLogger;

  constructor(
    private readonly request: APIRequestContext,
    private readonly gmsToken: string,
    private readonly gmsUrl: string,
    logger?: DataHubLogger,
  ) {
    this.logger = logger ?? createScriptLogger('test-data');
  }

  async load(featureDir: string, featureName: string): Promise<void> {
    if (process.env.PW_NO_SETUP === '1') {
      this.logger.info(`Skipping injection for '${featureName}' (PW_NO_SETUP=1)`);
      return;
    }

    const dataFile = path.join(featureDir, 'data', `${featureName}.json`);
    if (!fs.existsSync(dataFile)) {
      throw new Error(
        `Feature data file not found: ${dataFile}\n` + `Expected location: {featureDir}/data/${featureName}.json`,
      );
    }

    const featurePrefix = `pw_${featureName}_`;
    const alreadySeeded = await prefixExists(this.request, this.gmsUrl, featurePrefix, this.gmsToken);
    if (alreadySeeded) {
      this.logger.info(`Entities with prefix '${featurePrefix}' already exist — skipping injection`);
      return;
    }

    const mcps = JSON.parse(fs.readFileSync(dataFile, 'utf-8')) as Mcp[];
    this.logger.info(`Injecting ${mcps.length} entities for feature '${featureName}'...`);

    for (const mcp of mcps) {
      const urn = extractUrn(mcp);
      const response = await this.request.post(`${this.gmsUrl}/entities?action=ingest`, {
        data: { entity: { value: mcp.proposedSnapshot } },
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.gmsToken}`,
        },
      });
      if (!response.ok()) {
        const body = await response.text();
        throw new Error(`Failed to ingest entity ${urn}: ${response.status()} ${response.statusText()}\n${body}`);
      }
    }

    this.logger.info(`Injection complete for '${featureName}' (${mcps.length} entities)`);
  }
}
