import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

/**
 * The "Columns" control on a dataset's Schema tab: the popover with the ad hoc column toggles,
 * the saved Column Views, and the Create / Save-as builder modal.
 */
export class ColumnViewsPage extends BasePage {
  readonly columnsButton: Locator;
  readonly saveAsButton: Locator;
  readonly resetButton: Locator;
  readonly builderModal: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.columnsButton = page.getByTestId('column-view-select');
    this.saveAsButton = page.getByRole('button', { name: 'Save as Column View' });
    this.resetButton = page.getByRole('button', { name: 'Reset' });
    this.builderModal = page.getByRole('dialog');
  }

  async navigateToSchema(datasetUrn: string): Promise<void> {
    this.logger?.step('navigateToSchema', { datasetUrn });
    await this.navigate(`/dataset/${encodeURIComponent(datasetUrn)}/Schema`);
    await this.waitForPageLoad();
    await expect(this.columnsButton).toBeVisible();
  }

  header(label: string): Locator {
    return this.page.getByRole('columnheader', { name: label });
  }

  columnToggle(label: string): Locator {
    return this.page.getByRole('checkbox', { name: label });
  }

  /** The open popover's content (antd keeps a closed popover mounted but hidden). */
  get popover(): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- antd Popover has no test id hook
    return this.page.locator('.ant-popover:not(.ant-popover-hidden)');
  }

  /** Idempotent: the Columns button toggles, so never click when the popover is already open. */
  async openPopover(): Promise<void> {
    this.logger?.step('openPopover');
    if (!(await this.popover.isVisible())) {
      await this.columnsButton.click();
    }
    await expect(this.popover.getByRole('button', { name: 'Save as Column View' })).toBeVisible();
  }

  async closePopover(): Promise<void> {
    if (await this.popover.isVisible()) {
      await this.page.keyboard.press('Escape');
      await expect(this.popover).toBeHidden();
    }
  }

  /** Switch back to the built-in default layout via the popover's "Default" entry. */
  async selectBuiltInDefault(): Promise<void> {
    this.logger?.step('selectBuiltInDefault');
    await this.openPopover();
    await this.popover.getByText('Default', { exact: true }).click();
    await this.expectActiveViewLabel('Default');
  }

  async expectActiveViewLabel(name: string, modified = false): Promise<void> {
    await expect(this.columnsButton).toHaveText(`Columns: ${name}${modified ? ' •' : ''}`);
  }

  async toggleColumn(label: string): Promise<void> {
    this.logger?.step('toggleColumn', { label });
    await this.columnToggle(label).click();
  }

  /**
   * Save the current ad hoc layout as a named personal Column View. Returns the new view's urn,
   * taken from the `createColumnView` response — listing endpoints are search-backed and lag the write.
   */
  async saveAsColumnView(name: string): Promise<string> {
    this.logger?.step('saveAsColumnView', { name });
    await this.saveAsButton.click();
    await expect(this.builderModal).toBeVisible();
    await this.builderModal.getByRole('textbox').first().fill(name);
    const created = this.page.waitForResponse(
      (r) => r.url().includes('/graphql') && (r.request().postData() ?? '').includes('createColumnView'),
    );
    await this.builderModal.getByRole('button', { name: 'Save', exact: true }).click();
    const body = (await (await created).json()) as { data?: { createColumnView?: { urn: string } } };
    await expect(this.builderModal).toBeHidden();
    const urn = body.data?.createColumnView?.urn;
    if (!urn) throw new Error(`createColumnView returned no urn: ${JSON.stringify(body).slice(0, 300)}`);
    return urn;
  }

  /** A saved view entry in the popover (antd Typography.Link renders an href-less <a>, so no `link` role). */
  savedViewLink(name: string): Locator {
    return this.popover.getByText(name, { exact: true });
  }

  /** The "+ Add structured property…" picker inside the open popover (an antd multi-select). */
  get structuredPropertyPicker(): Locator {
    return this.popover.getByRole('combobox');
  }

  /** Add a structured property as a column through the picker, by its display name. */
  async addStructuredPropertyColumn(displayName: string): Promise<void> {
    this.logger?.step('addStructuredPropertyColumn', { displayName });
    await this.structuredPropertyPicker.click();
    await this.structuredPropertyPicker.fill(displayName);
    // antd's visible dropdown items have no `option` role (only its hidden a11y list does, with the
    // urn as text); the picker sets each item's `title` to the display name.
    await this.page.getByTitle(displayName, { exact: true }).click();
    await this.page.keyboard.press('Escape'); // close the dropdown, keep the popover
  }
}
