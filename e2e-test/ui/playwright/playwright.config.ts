import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [
    ['html'],
    ['junit', { outputFile: 'test-results/junit.xml' }],
  ],
  use: {
    baseURL: process.env.BASE_URL || 'http://localhost:9002',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    // storageState is intentionally NOT set here.
    // loginFixture in base-test.ts overrides the `context` fixture to manage
    // auth state per-worker: checks .auth/{username}.json, logs in via UI on
    // first run, saves state, reuses on subsequent runs.
    // login-test.ts starts with a clean unauthenticated context for login tests.
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
      testIgnore: /.*\.setup\.ts/,
    },
  ],

  webServer: {
    command: 'cd ../../.. && ./gradlew quickstartDebug',
    url: 'http://localhost:9002',
    reuseExistingServer: !process.env.CI,
    timeout: 120 * 1000,
  },
});
