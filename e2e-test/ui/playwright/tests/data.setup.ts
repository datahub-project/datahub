import { test as setup } from '@playwright/test';
import { exec } from 'child_process';
import { promisify } from 'util';
import { resolve } from 'path';
import * as fs from 'fs';

const execAsync = promisify(exec);

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
  console.log('📦 Setting up all test data via DataHub CLI...');

  // Load GMS token for authentication
  const tokenData = JSON.parse(fs.readFileSync(tokenFile, 'utf-8'));
  const gmsToken = tokenData.token;

  if (!gmsToken) {
    throw new Error('GMS token not found. Auth setup may have failed.');
  }

  const gmsUrl = process.env.BASE_URL?.replace(':9002', ':8080') || 'http://localhost:8080';
  console.log(`🔑 Using GMS URL: ${gmsUrl}`);

  try {
    // Fixture files to ingest
    const fixtures = [
      resolve(__dirname, 'business-attributes/fixtures/data.json'),
      resolve(__dirname, 'search/fixtures/data.json'),
      resolve(__dirname, 'incidents-v2/fixtures/data.json'),
    ];

    const allUrns: string[] = [];

    for (const fixturePath of fixtures) {
      console.log(`📂 Ingesting fixture: ${fixturePath}`);

      // Extract URNs from fixture for tracking
      const fixtureData = JSON.parse(fs.readFileSync(fixturePath, 'utf-8'));
      const urns = fixtureData.map((mcp: any) => {
        if (mcp.entityUrn) return mcp.entityUrn;
        if (mcp.proposedSnapshot) {
          const snapshot = Object.values(mcp.proposedSnapshot)[0] as any;
          return snapshot?.urn;
        }
        return null;
      }).filter(Boolean);

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

      console.log(`🚀 Ingesting ${urns.length} entities via CLI...`);

      const command = `datahub ingest -c ${configPath}`;

      const { stdout, stderr } = await execAsync(command, {
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
        console.warn(`⚠️  CLI stderr: ${stderr}`);
      }

      console.log(`✅ Successfully ingested ${urns.length} entities`);
    }

    // Store URNs in global scope for cleanup
    (global as any).__ALL_TEST_URNS = allUrns;

    console.log('✅ All test data ready');
    console.log(`📝 Seeded ${allUrns.length} total entities`);
  } catch (error: any) {
    console.error('❌ Failed to seed test data:', error.message);
    if (error.stdout) console.error('stdout:', error.stdout);
    if (error.stderr) console.error('stderr:', error.stderr);
    throw error;
  }
});
