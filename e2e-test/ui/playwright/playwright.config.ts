import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 1 : 4,
  reporter: process.env.CI
    ? [
        ['blob'],
        ['html'],
        ['junit', { outputFile: 'test-results/junit.xml' }],
      ]
    : [
        ['html'],
        ['junit', { outputFile: 'test-results/junit.xml' }],
      ],
  use: {
    baseURL: process.env.BASE_URL || 'http://localhost:9002',
    trace: 'retain-on-failure',
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
    // PLAYWRIGHT_REUSE_SERVER=1 lets the Docker-based CI pipeline start DataHub
    // externally (via run-quickstart.sh) and reuse it, rather than re-launching
    // via quickstartDebug and conflicting on port 9002.
    reuseExistingServer: process.env.PLAYWRIGHT_REUSE_SERVER === '1' || !process.env.CI,
    timeout: 120 * 1000,
  },
});
