import { test as setup } from '@playwright/test';
import { CleanupHelper } from '../helpers/CleanupHelper';

/**
 * Global Setup - Runs ONCE before ALL test suites
 *
 * This setup file performs global cleanup to ensure a clean state before tests run.
 * It removes any leftover test data from previous runs that may have failed cleanup.
 *
 * This is especially important for:
 * - Cypress test entities (Cypress*, cypress_*)
 * - Test entities from failed runs
 * - Ensuring consistent test environment
 */

setup('global cleanup', async ({ page }) => {
  console.log('🧹 Starting global cleanup...');

  const cleanup = new CleanupHelper(page);

  try {
    // Clean up all test data patterns
    await cleanup.cleanupTestData();

    console.log('✅ Global cleanup complete - test environment is ready');
  } catch (error) {
    // Don't fail tests if cleanup fails - log warning instead
    console.warn('⚠️  Global cleanup encountered errors:', error);
    console.warn('⚠️  Continuing with tests...');
  }
});
