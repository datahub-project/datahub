import { Locator, Page, Route } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../fixtures/logger.fixture';

/** Snapshot of the rows currently mounted by the virtualised schema table. */
export interface MountedRows {
  /** fieldPaths of the rows in the DOM, in table order. */
  fieldPaths: string[];
  /** Rows whose description cell is absent or empty. */
  missingDescriptions: string[];
  /** Metadata skeleton placeholders still showing. */
  skeletons: number;
}

/**
 * Schema tab of a dataset, as driven by the scale smoke test. Owns the handful of raw
 * locators the virtualised table needs (the antd scroll body and its row keys), so the
 * spec itself stays on data-testids.
 */
export class SchemaScalePage extends BasePage {
  /** The Columns tab header; its pill shows the column count. */
  readonly tabHeader: Locator;
  readonly tableContainer: Locator;
  /** The antd table body: the element that actually scrolls and that the virtualiser watches. */
  readonly scrollBody: Locator;
  /** antd pagination controls; the schema table is virtualised and must never show them. */
  readonly paginationControls: Locator;
  readonly fieldDrawer: Locator;
  readonly metadataErrorBanner: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.tabHeader = page.getByTestId('Columns-entity-tab-header');
    this.tableContainer = page.getByTestId('schema-table-container');
    // eslint-disable-next-line playwright/no-raw-locators -- antd renders the scrollable body without a testid
    this.scrollBody = this.tableContainer.locator('.ant-table-body');
    // eslint-disable-next-line playwright/no-raw-locators -- antd pagination has no testid
    this.paginationControls = this.tableContainer.locator('.ant-pagination');
    this.fieldDrawer = page.getByTestId('schema-field-drawer-content');
    this.metadataErrorBanner = page.getByTestId('metadata-error-banner');
  }

  /** Navigate straight to the Columns (schema) tab; the caller measures from before this call. */
  async gotoSchemaTab(datasetUrn: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/Columns`);
  }

  retryButton(): Locator {
    return this.metadataErrorBanner.getByRole('button', { name: /retry/i });
  }

  /** The GraphQL operation name of a request, if it is one. */
  static operationName(postData: string | null): string | undefined {
    try {
      return postData ? (JSON.parse(postData) as { operationName?: string }).operationName : undefined;
    } catch {
      return undefined;
    }
  }

  /** Resolves when the browser sends the named GraphQL operation. Arm it before navigating. */
  waitForOperation(operationName: string, timeout: number): Promise<unknown> {
    return this.page.waitForRequest(
      (req) => req.url().includes('/graphql') && SchemaScalePage.operationName(req.postData()) === operationName,
      { timeout },
    );
  }

  /**
   * Abort the named GraphQL operation while `isBlocked()` returns true; other requests pass.
   * Returns a function that removes the route again.
   */
  async blockOperation(operationName: string, isBlocked: () => boolean): Promise<() => Promise<void>> {
    const handler = async (route: Route) => {
      if (isBlocked() && SchemaScalePage.operationName(route.request().postData()) === operationName) {
        await route.abort();
        return;
      }
      await route.continue();
    };
    await this.page.route(/\/api\/v2\/graphql/, handler);
    return () => this.page.unroute(/\/api\/v2\/graphql/, handler);
  }

  row(fieldPath: string): Locator {
    return this.page.getByTestId(`schema-field-${fieldPath}`);
  }

  /**
   * Open a column's drawer. The row's onClick (SchemaTable onRow) toggles the drawer, so a
   * dispatched click is enough. A pointer click is deliberately avoided: the row is wider
   * than the viewport and Playwright's scroll-into-view fights the virtualiser, leaving the
   * target under the sticky header until the action times out.
   */
  async clickRow(fieldPath: string, timeout: number): Promise<void> {
    this.logger?.step('clickRow', { fieldPath });
    await this.row(fieldPath).dispatchEvent('click', undefined, { timeout });
  }

  description(fieldPath: string): Locator {
    return this.page.getByTestId(`schema-field-${fieldPath}-description`);
  }

  /** The drawer's Properties tab lists every structured property assigned to the column. */
  async openDrawerPropertiesTab(timeout: number): Promise<void> {
    this.logger?.step('openDrawerPropertiesTab');
    await this.fieldDrawer.getByTestId('Properties-field-drawer-tab-header').click({ timeout });
  }

  /** Properties are grouped by namespace and collapsed; expand one group by its name. */
  async expandDrawerPropertyGroup(namespace: string, timeout: number): Promise<void> {
    this.logger?.step('expandDrawerPropertyGroup', { namespace });
    // The row's accessible name is "right <namespace> (<count>)"; the chevron is the toggle.
    await this.fieldDrawer
      .getByRole('row', { name: new RegExp(`\\b${namespace} \\(\\d+\\)`) })
      .getByRole('img', { name: 'right' })
      .click({ timeout });
  }

  /** The Properties-tab row for one structured property, matched on its exact display name. */
  drawerPropertyRow(displayName: string): Locator {
    return this.fieldDrawer.getByRole('row').filter({ has: this.page.getByText(displayName, { exact: true }) });
  }

  /** Scroll the virtualised body to the given fraction of its total height. */
  async scrollToFraction(fraction: number): Promise<void> {
    this.logger?.step('scrollToFraction', { fraction });
    await this.scrollBody.evaluate((el, f) => {
      el.scrollTop = Math.floor((el.scrollHeight - el.clientHeight) * f);
    }, fraction);
  }

  /** What the virtualiser has mounted right now, and whether its metadata has arrived. */
  async mountedRows(): Promise<MountedRows> {
    return this.tableContainer.evaluate((root) => {
      const rows = Array.from(root.querySelectorAll<HTMLElement>('tr[data-row-key]'));
      const fieldPaths = rows.map((r) => r.dataset.rowKey ?? '');
      const missingDescriptions = rows
        .filter((r) => {
          const cell = r.querySelector('[data-testid$="-description"]');
          return !cell || (cell.textContent ?? '').trim() === '';
        })
        .map((r) => r.dataset.rowKey ?? '');
      const skeletons = root.querySelectorAll('[data-testid="metadata-cell-skeleton"]').length;
      return { fieldPaths, missingDescriptions, skeletons };
    });
  }
}
