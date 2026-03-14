import { test as setup } from '@playwright/test';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs';
import * as path from 'path';

const execAsync = promisify(exec);

/**
 * Global Setup - Runs ONCE before ALL test suites
 *
 * This setup file performs global CLI-based cleanup to ensure a clean state before tests run.
 * It removes any leftover test data from previous runs using datahub CLI.
 *
 * This is especially important for:
 * - Playwright test entities (Playwright*, playwright*, SamplePlaywright*, fct_playwright*)
 * - Legacy Cypress test entities (Cypress*, cypress*)
 * - Test entities from failed runs
 * - Ensuring consistent test environment
 */

setup('global cleanup', async () => {
  setup.setTimeout(180000); // 3 minute timeout for cleanup
  console.log('🧹 Starting CLI-based global cleanup...');

  try {
    // Read GMS token for authentication
    const tokenFile = path.join(__dirname, '../.auth/gms-token.json');
    let token: string | undefined;

    if (fs.existsSync(tokenFile)) {
      const tokenData = JSON.parse(fs.readFileSync(tokenFile, 'utf-8'));
      token = tokenData.token;
      console.log('🔑 Using existing GMS token for cleanup');
    } else {
      console.log('⚠️  No GMS token found, cleanup may require authentication');
    }

    // Configure datahub CLI
    const datahubEnv = {
      DATAHUB_GMS_URL: 'http://localhost:8080',
      ...(token && { DATAHUB_GMS_TOKEN: token }),
    };

    // Search patterns for test entities
    const patterns = [
      'cypress',
      'Cypress',
      'Test',
      'Playwright',
      'playwright',
      'SamplePlaywright',
      'fct_playwright',
    ];

    let totalDeleted = 0;
    const urnsToDelete = new Set<string>();

    // Collect all URNs to delete (only for dataset entity type to speed up)
    for (const pattern of patterns) {
      try {
        const query = `${pattern}*`;
        console.log(`🔍 Searching for entities matching: ${query}`);

        // Use datahub CLI to search (with dataset as primary entity type)
        const searchCmd = `datahub delete by-filter --query "${query}" --entity-type dataset --force --hard --dry-run`;

        const { stdout, stderr } = await execAsync(searchCmd, {
          env: { ...process.env, ...datahubEnv },
          timeout: 30000,
        });

        const output = stdout + stderr;

        // Extract URNs from output
        const urnMatches = output.match(/urn:li:[a-zA-Z]+:\([^)]+\)/g);
        if (urnMatches) {
          urnMatches.forEach((urn) => urnsToDelete.add(urn));
          console.log(`   Found ${urnMatches.length} entities`);
        }
      } catch (error: any) {
        const errorMsg = error.message || '';
        if (!errorMsg.includes('No entities found') && !errorMsg.includes('0 entities')) {
          console.warn(`   ⚠️  Search failed for pattern ${pattern}*:`, errorMsg.split('\n')[0]);
        }
      }
    }

    // Also search for tags and glossary terms
    for (const entityType of ['tag', 'glossaryTerm']) {
      for (const pattern of patterns) {
        try {
          const query = `${pattern}*`;
          const searchCmd = `datahub delete by-filter --query "${query}" --entity-type ${entityType} --force --hard --dry-run`;

          const { stdout, stderr } = await execAsync(searchCmd, {
            env: { ...process.env, ...datahubEnv },
            timeout: 20000,
          });

          const output = stdout + stderr;
          const urnMatches = output.match(/urn:li:[a-zA-Z]+:[^,\s\n\]]+/g);
          if (urnMatches) {
            urnMatches.forEach((urn) => urnsToDelete.add(urn.trim()));
          }
        } catch (error: any) {
          // Silent fail for non-critical entity types
        }
      }
    }

    if (urnsToDelete.size > 0) {
      console.log(`📝 Collected ${urnsToDelete.size} unique URNs to delete`);

      // Write URNs to file
      const urnsFile = path.join(__dirname, '.cleanup-urns.txt');
      fs.writeFileSync(urnsFile, Array.from(urnsToDelete).join('\n'));

      // Delete using URN file
      console.log('🗑️  Deleting entities...');
      const deleteCmd = `datahub delete by-filter --urn-file ${urnsFile} --force --hard`;

      try {
        await execAsync(deleteCmd, {
          env: { ...process.env, ...datahubEnv },
          timeout: 120000,
        });

        totalDeleted = urnsToDelete.size;
        console.log(`✅ Successfully deleted ${totalDeleted} test entities`);
      } catch (error: any) {
        console.warn('⚠️  Deletion encountered errors:', error.message.split('\n')[0]);
      }

      // Cleanup temp file
      fs.unlinkSync(urnsFile);
    } else {
      console.log('✅ No test entities found to clean up');
    }
  } catch (error: any) {
    // Don't fail tests if cleanup fails - log warning instead
    console.warn('⚠️  Global cleanup encountered errors:', error.message);
    console.warn('⚠️  Continuing with tests...');
  }
});
