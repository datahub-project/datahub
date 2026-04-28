import { test as setup } from '@playwright/test';
import { exec } from 'child_process';
import { promisify } from 'util';
import { resolve } from 'path';
import * as fs from 'fs';
import { createScriptLogger } from '../utils/logger';

const execAsync = promisify(exec);
const logger = createScriptLogger('data.setup');

/**
 * Consolidated Data Setup
 *
 * Runs ONCE before ALL tests to seed all test data via DataHub CLI.
 * Uses the datahub CLI which handles format compatibility automatically.
 *
 * Seeded entities:
 * - Tags, GlossaryNodes, GlossaryTerms
 * - Datasets with schemas
 * - BusinessAttributes
 */

const tokenFile = '.auth/gms-token-datahub.json';

setup('seed all test data', async () => {
  logger.info('Setting up all test data via DataHub CLI...');

  // Load GMS token for authentication
  const tokenData = JSON.parse(fs.readFileSync(tokenFile, 'utf-8'));
  const gmsToken = tokenData.token;

  if (!gmsToken) {
    throw new Error('GMS token not found. Auth setup may have failed.');
  }

  const gmsUrl = process.env.BASE_URL?.replace(':9002', ':8080') || 'http://localhost:8080';
  logger.info('Using GMS URL', { gmsUrl });

  try {
    // Fixture files to ingest
    const fixtures = [
      resolve(__dirname, 'business-attributes/fixtures/data.json'),
      resolve(__dirname, 'search/fixtures/data.json'),
      resolve(__dirname, 'incidents-v2/fixtures/data.json'),
    ];

    const allUrns: string[] = [];

    for (const fixturePath of fixtures) {
      logger.info('Ingesting fixture', { path: fixturePath });

      // Extract URNs from fixture for tracking
      const fixtureData = JSON.parse(fs.readFileSync(fixturePath, 'utf-8'));
      const urns = fixtureData
        .map((mcp: { entityUrn?: string; proposedSnapshot?: Record<string, unknown> }) => {
          if (mcp.entityUrn) return mcp.entityUrn;
          if (mcp.proposedSnapshot) {
            const snapshot = Object.values(mcp.proposedSnapshot)[0] as { urn?: string } | undefined;
            return snapshot?.urn;
          }
          return null;
        })
        .filter(Boolean);

      allUrns.push(...urns);

      // Write CLI config to temporary file
      const configPath = resolve(__dirname, '../../../..', `.ingest-config-${Date.now()}.json`);
      const config = {
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
            token: gmsToken,
          },
        },
      };

      fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

      logger.info(`Ingesting ${urns.length} entities via CLI...`);

      const command = `datahub ingest -c ${configPath}`;

      const { stdout: _stdout, stderr } = await execAsync(command, {
        env: {
          ...process.env,
          DATAHUB_GMS_URL: gmsUrl,
          DATAHUB_GMS_TOKEN: gmsToken,
          DATAHUB_TELEMETRY_ENABLED: 'false',
        },
        maxBuffer: 10 * 1024 * 1024, // 10MB buffer
        cwd: resolve(__dirname, '../../../..'), // Run from repo root
      });

      // Cleanup config file
      try {
        fs.unlinkSync(configPath);
      } catch (e) {
        // Ignore cleanup errors
      }

      if (stderr && !stderr.includes('WARNING') && !stderr.includes('INFO')) {
        logger.warn('CLI stderr output', { stderr });
      }

      logger.info(`Successfully ingested ${urns.length} entities`);
    }

    // Store URNs in global scope for cleanup
    (global as { __ALL_TEST_URNS?: string[] }).__ALL_TEST_URNS = allUrns;

    logger.info('All test data ready');
    logger.info(`Seeded ${allUrns.length} total entities`);
  } catch (error) {
    const err = error as { message: string; stdout?: string; stderr?: string };
    logger.error('Failed to seed test data', { message: err.message });
    if (err.stdout) logger.error('stdout', { stdout: err.stdout });
    if (err.stderr) logger.error('stderr', { stderr: err.stderr });
    throw error;
  }
});
