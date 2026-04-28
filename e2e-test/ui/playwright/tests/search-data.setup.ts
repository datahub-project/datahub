import { test as setup } from '@playwright/test';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs';
import * as path from 'path';
import { createScriptLogger } from '../utils/logger';

const execAsync = promisify(exec);
const logger = createScriptLogger('search-data.setup');

setup('seed search data', async () => {
  setup.setTimeout(180000); // 3 minute timeout
  logger.info('Seeding search test data via datahub CLI...');

  try {
    // Read GMS token
    const tokenFile = path.join(__dirname, '../.auth/gms-token-datahub.json');
    const tokenData = JSON.parse(fs.readFileSync(tokenFile, 'utf-8'));
    const token = tokenData.token;

    // Create config with token for Playwright data (has all entity types)
    const playwrightDataConfig = {
      source: {
        type: 'file',
        config: {
          filename: '../fixtures/data.json',
        },
      },
      sink: {
        type: 'datahub-rest',
        config: {
          server: 'http://localhost:8080',
          token: token,
        },
      },
    };

    const playwrightDataConfigPath = path.join(__dirname, 'playwright-ingest-config-with-token.json');
    fs.writeFileSync(playwrightDataConfigPath, JSON.stringify(playwrightDataConfig, null, 2));

    // Ingest Playwright test data (Dashboards, ML Models, Pipelines, HDFS, etc.)
    logger.info('Ingesting Playwright data.json (107 entities with all types)...');
    const { stdout: stdout1 } = await execAsync(`datahub ingest -c ${playwrightDataConfigPath}`, {
      cwd: path.resolve(__dirname),
      timeout: 120000,
    });

    if (stdout1.includes('failed')) {
      throw new Error(`Ingestion failed: ${stdout1}`);
    }
    logger.info('Seeded Playwright data');

    // Create config with token for Playwright search data.json
    const searchDataConfig = {
      source: {
        type: 'file',
        config: {
          filename: './search/fixtures/data.json',
        },
      },
      sink: {
        type: 'datahub-rest',
        config: {
          server: 'http://localhost:8080',
          token: token,
        },
      },
    };

    const searchDataConfigPath = path.join(__dirname, 'playwright-search-ingest-config-with-token.json');
    fs.writeFileSync(searchDataConfigPath, JSON.stringify(searchDataConfig, null, 2));

    // Ingest Playwright search test data
    logger.info('Ingesting Playwright search/fixtures/data.json...');
    const { stdout: stdout2 } = await execAsync(`datahub ingest -c ${searchDataConfigPath}`, {
      cwd: path.resolve(__dirname),
      timeout: 60000,
    });

    if (stdout2.includes('failed')) {
      throw new Error(`Ingestion failed: ${stdout2}`);
    }
    logger.info('Seeded Playwright search fixtures data');

    // Create config with token for business-attributes data.json
    const businessAttributesConfig = {
      source: {
        type: 'file',
        config: {
          filename: './business-attributes/fixtures/data.json',
        },
      },
      sink: {
        type: 'datahub-rest',
        config: {
          server: 'http://localhost:8080',
          token: token,
        },
      },
    };

    const businessAttributesConfigPath = path.join(__dirname, 'business-attributes-ingest-config-with-token.json');
    fs.writeFileSync(businessAttributesConfigPath, JSON.stringify(businessAttributesConfig, null, 2));

    // Ingest business-attributes data
    logger.info('Ingesting business-attributes/fixtures/data.json...');
    const { stdout: stdout3 } = await execAsync(`datahub ingest -c ${businessAttributesConfigPath}`, {
      cwd: path.resolve(__dirname),
      timeout: 60000,
    });

    if (stdout3.includes('failed')) {
      throw new Error(`Ingestion failed: ${stdout3}`);
    }
    logger.info('Seeded business-attributes fixtures data');

    // Cleanup temp config files
    fs.unlinkSync(playwrightDataConfigPath);
    fs.unlinkSync(searchDataConfigPath);
    fs.unlinkSync(businessAttributesConfigPath);

    // Wait for Elasticsearch indexing
    logger.info('Waiting for search indexing (15 seconds)...');
    await new Promise((resolve) => setTimeout(resolve, 15000));

    logger.info('Search test data seeded successfully');
  } catch (error) {
    const err = error as { message: string };
    logger.error('Failed to seed data', { message: err.message });
    throw error;
  }
});
