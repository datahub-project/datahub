/**
 * Lightweight config for local dev runs against an already-running DataHub.
 * Use:  npx playwright test --config=playwright.no-webserver.config.ts <spec>
 *
 * Differs from playwright.config.ts only in that it omits the webServer
 * block, so Playwright never tries to launch quickstartDebug. Useful when
 * iterating on a single spec against an existing instance.
 */
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  timeout: 60 * 1000,
  fullyParallel: false,
  forbidOnly: false,
  retries: 0,
  workers: 1,
  reporter: [['list']],
  use: {
    baseURL: process.env.BASE_URL || 'http://localhost:9002',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'off',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
      testIgnore: /.*\.setup\.ts/,
    },
  ],
});
