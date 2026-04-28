/**
 * Global Setup — runs ONCE before all test projects (after auth-setup).
 *
 * Deletes leftover test entities from previous runs so that data-seeding
 * starts from a clean state. Uses the GMS REST API directly — no CLI required.
 *
 * Entities are matched by the `pw_` prefix convention enforced by test-data.ts.
 * Additional legacy prefixes (Cypress, fct_cypress, etc.) are also included so
 * old test runs don't leave behind orphaned data.
 */

import { test as setup, request } from '@playwright/test';
import { readGmsToken } from '../fixtures/login';
import { deleteEntities } from '../utils/cleanup';
import { gmsUrl } from '../utils/constants';
import { createScriptLogger } from '../utils/logger';

const logger = createScriptLogger('global.setup');

const ADMIN_USERNAME = 'datahub';

/** Search prefixes to purge before each run. */
const CLEANUP_PREFIXES = ['pw_', 'cypress_', 'Cypress', 'fct_cypress', 'SampleCypress'];

/** Entity types to search across. */
const ENTITY_TYPES = ['DATASET', 'TAG', 'GLOSSARY_TERM', 'DOMAIN', 'DATA_PRODUCT'];

setup('global cleanup', async () => {
  setup.setTimeout(180_000);
  logger.info('Starting global pre-run cleanup...');

  const url = gmsUrl();
  let gmsToken: string;

  try {
    gmsToken = readGmsToken(ADMIN_USERNAME);
  } catch {
    logger.warn('GMS token not found — skipping global cleanup');
    return;
  }

  const apiContext = await request.newContext({
    baseURL: url,
    extraHTTPHeaders: { Authorization: `Bearer ${gmsToken}` },
  });

  try {
    const urnsToDelete: string[] = [];

    for (const entityType of ENTITY_TYPES) {
      for (const prefix of CLEANUP_PREFIXES) {
        const response = await apiContext.post(`${url}/api/v2/graphql`, {
          data: {
            query: `query search($input: SearchInput!) { search(input: $input) { total entities { urn } } }`,
            variables: {
              input: { type: entityType, query: `${prefix}*`, start: 0, count: 200 },
            },
          },
          headers: { 'Content-Type': 'application/json' },
        });

        if (!response.ok()) continue;

        const json = (await response.json()) as {
          data?: { search?: { total?: number; entities?: { urn: string }[] } };
        };
        const entities = json.data?.search?.entities ?? [];
        if (entities.length > 0) {
          logger.info(`Found ${entities.length} ${entityType} entities matching '${prefix}*'`);
          urnsToDelete.push(...entities.map((e) => e.urn));
        }
      }
    }

    if (urnsToDelete.length === 0) {
      logger.info('No leftover test entities found');
      return;
    }

    logger.info(`Deleting ${urnsToDelete.length} leftover entities...`);
    await deleteEntities(apiContext, url, urnsToDelete, logger);
    logger.info('Global cleanup complete');
  } finally {
    await apiContext.dispose();
  }
});
