/**
 * BasePage — root class for all DataHub page objects.
 *
 * Accepts an optional DataHubLogger and logDir so that page actions can emit
 * structured log entries without tests needing to pass the logger explicitly
 * on every method call.
 *
 * screenshot(name) provides a standardised checkpoint capture. The output path
 * is formed automatically from logDir so callers only need to supply a name —
 * no hardcoded paths in test code.
 *
 * Backward compatibility:
 *   logger and logDir are optional. Existing page objects that don't pass them
 *   continue to work; they just won't emit structured logs or screenshots.
 */

import * as path from 'path';
import type { Page } from '@playwright/test';
import type { DataHubLogger } from '../utils/logger';

export class BasePage {
  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {}

  async navigate(url: string): Promise<void> {
    this.logger?.step('navigate', { url });
    await this.page.goto(url);
  }

  async waitForPageLoad(): Promise<void> {
    await this.page.waitForLoadState('networkidle');
  }

  async getTitle(): Promise<string> {
    return this.page.title();
  }

  /**
   * Capture a named screenshot checkpoint. The file is written to logDir so
   * it lands alongside the structured logs for this test. Falls back to
   * Playwright's default output directory when logDir is not provided.
   */
  async screenshot(name: string): Promise<void> {
    const screenshotPath = this.logDir
      ? path.join(this.logDir, `${name}.png`)
      : `${name}.png`;
    this.logger?.step('screenshot', { name, path: screenshotPath });
    await this.page.screenshot({ path: screenshotPath, fullPage: false });
  }
}
