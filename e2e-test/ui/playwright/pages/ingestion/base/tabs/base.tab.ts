import { expect, Locator, Page } from '@playwright/test';
import { DataHubLogger } from '@utils/logger';
import { ToastComponent } from '@pages/common/toast-component';

export abstract class BaseTab {
  abstract readonly name: string;
  abstract readonly path: string;
  abstract readonly tabKey: string;

  protected readonly toast: ToastComponent;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    this.toast = new ToastComponent(page);
  }

  get tab(): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design tab node keyed by data-node-key; no ARIA role exposed
    return this.page.locator(`div[data-node-key="${this.tabKey}"]`);
  }

  async navigate(): Promise<void> {
    this.logger?.step(`navigate to ${this.name} tab`);
    await this.page.goto(this.path);
    await this.page.waitForLoadState('domcontentloaded');
    await this.page.waitForLoadState('networkidle');
    await this.tab.waitFor({ state: 'visible' });
  }

  async open(): Promise<void> {
    this.logger?.step(`open ${this.name} tab`);
    await this.tab.click();
    await this.expectTabActive();
  }

  async expectTabActive(): Promise<void> {
    await expect(this.tab).toHaveClass(/ant-tabs-tab-active/);
  }
}
