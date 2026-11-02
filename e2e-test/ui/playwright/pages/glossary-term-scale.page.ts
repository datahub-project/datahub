import { Locator, Page, Request, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../fixtures/logger.fixture';

/**
 * Glossary term profile as driven by the allowed-values scale smoke test: the header, the
 * Properties tab and the edit-value modal for a structured property with a large allowed-values
 * list. Owns the few raw locators the antd Select needs, so the spec stays on data-testids.
 */
export class GlossaryTermScalePage extends BasePage {
  readonly entityHeader: Locator;
  readonly propertiesTabHeader: Locator;
  readonly propertiesTable: Locator;
  readonly editModal: Locator;
  readonly modalUpdateButton: Locator;
  /** The SimpleSelect rendered for a single-select property with more than five allowed values. */
  readonly allowedValuesSelect: Locator;
  readonly allowedValuesSearchInput: Locator;
  readonly allowedValuesDropdown: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.entityHeader = page.getByTestId('entity-header-test-id');
    this.propertiesTabHeader = page.getByTestId('Properties-entity-tab-header');
    this.propertiesTable = page.getByTestId('entity-properties-table');
    this.editModal = page.getByRole('dialog');
    this.modalUpdateButton = page.getByTestId('add-update-structured-prop-on-entity-button');
    this.allowedValuesSelect = page.getByTestId('structured-property-single-select');
    this.allowedValuesDropdown = page.getByTestId('structured-property-single-select-dropdown');
    this.allowedValuesSearchInput = this.allowedValuesDropdown.getByTestId('dropdown-search-bar').getByRole('textbox');
  }

  /** The GraphQL operation name of a request body, if it is one. */
  static operationName(postData: string | null): string | undefined {
    try {
      return postData ? (JSON.parse(postData) as { operationName?: string }).operationName : undefined;
    } catch {
      return undefined;
    }
  }

  /** Navigate straight to the term; the caller measures from before this call. */
  async gotoTerm(termUrn: string): Promise<void> {
    this.logger?.step('gotoTerm', { termUrn });
    await this.page.goto(`/glossaryTerm/${encodeURIComponent(termUrn)}`);
  }

  async openPropertiesTab(timeout: number): Promise<void> {
    this.logger?.step('openPropertiesTab');
    await this.propertiesTabHeader.click({ timeout });
  }

  /**
   * The Properties-tab row for one structured property, matched on its display name. Scoped to
   * the page rather than the table testid, which not every release renders.
   */
  propertyRow(displayName: string): Locator {
    return this.page.getByRole('row').filter({ hasText: displayName });
  }

  /** Open the edit modal for a property through its row's more-options menu. */
  async openEditModal(displayName: string, timeout: number): Promise<void> {
    this.logger?.step('openEditModal', { displayName });
    await this.propertyRow(displayName).getByTestId('structured-prop-entity-more-icon').click({ timeout });
    await this.page.getByRole('menuitem').filter({ hasText: 'Edit' }).click({ timeout });
  }

  /**
   * Open the allowed-values dropdown and, when the Select is searchable, type to filter it.
   * Returns whether typing was possible: a read-only combobox means the user can only scroll.
   */
  async searchAllowedValues(query: string, timeout: number): Promise<boolean> {
    this.logger?.step('searchAllowedValues', { query });
    await this.allowedValuesSelect.click({ timeout });
    await expect(this.allowedValuesDropdown).toBeVisible({ timeout });
    const searchable = (await this.allowedValuesSearchInput.count()) > 0;
    if (searchable) await this.allowedValuesSearchInput.fill(query);
    return searchable;
  }

  /** One option in the open dropdown, by its value. */
  allowedValueOption(value: string): Locator {
    return this.allowedValuesDropdown.getByTestId(`option-${value}`);
  }

  /**
   * What a user without search has to do: wheel-scroll the virtualised list until the option is
   * rendered, or the deadline passes. Returns the number of wheel ticks it took.
   */
  async scrollDropdownUntilVisible(option: Locator, deadlineMs: number): Promise<number> {
    this.logger?.step('scrollDropdownUntilVisible');
    await this.allowedValuesDropdown.hover();
    const deadline = Date.now() + deadlineMs;
    let ticks = 0;
    while (!(await option.isVisible()) && Date.now() < deadline) {
      await this.page.mouse.wheel(0, 400);
      ticks += 1;
    }
    return ticks;
  }

  /** Options currently rendered in the open dropdown. */
  renderedOptions(): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- options carry per-value testids; match the prefix
    return this.allowedValuesDropdown.locator('[data-testid^="option-"]');
  }

  /** The expanded content of one asset-summary sidebar section, by its title. */
  sidebarSectionContent(title: string): Locator {
    return this.page.getByTestId(`sidebar-section-content-${title}`);
  }

  /** One rendered structured-property value in the sidebar or Properties tab. */
  propertyValue(propertyName: string, value: string): Locator {
    return this.page.getByTestId(`property-${propertyName}-value-${value}`);
  }

  /** Type into the filter box that appears above a property's values once they exceed one page. */
  async filterPropertyValues(propertyName: string, query: string, timeout: number): Promise<void> {
    this.logger?.step('filterPropertyValues', { propertyName, query });
    const box = this.page.getByTestId(`property-${propertyName}-values-filter`);
    await expect(box).toBeVisible({ timeout });
    const input = box.getByRole('textbox');
    await ((await input.count()) > 0 ? input : box).fill(query);
  }

  /**
   * Record every GraphQL request from now on: operation, request/response bytes and duration.
   * `settled()` resolves once no GraphQL request has been in flight for `idleMs`.
   */
  recordGraphql(): GraphqlRecorder {
    return new GraphqlRecorder(this.page);
  }

  async closeModal(): Promise<void> {
    this.logger?.step('closeModal');
    await this.page.keyboard.press('Escape');
  }
}

export interface GraphqlCall {
  operation: string;
  startedAt: number;
  durationMs: number;
  requestBytes: number;
  responseBytes: number;
  status: number;
}

export class GraphqlRecorder {
  readonly calls: GraphqlCall[] = [];
  private inFlight = 0;
  private lastActivity = Date.now();
  private readonly started = new Map<Request, { operation: string; startedAt: number; requestBytes: number }>();

  constructor(private readonly page: Page) {
    page.on('request', (req) => {
      if (!req.url().includes('/graphql')) return;
      const postData = req.postData() ?? '';
      this.started.set(req, {
        operation: GlossaryTermScalePage.operationName(postData) ?? 'anonymous',
        startedAt: Date.now(),
        requestBytes: postData.length,
      });
      this.inFlight += 1;
      this.lastActivity = Date.now();
    });
    const finish = async (req: Request) => {
      const meta = this.started.get(req);
      if (!meta) return;
      this.started.delete(req);
      const res = await req.response();
      let responseBytes = 0;
      try {
        responseBytes = res ? (await res.body()).length : 0;
      } catch {
        responseBytes = -1;
      }
      this.calls.push({
        ...meta,
        durationMs: Date.now() - meta.startedAt,
        responseBytes,
        status: res?.status() ?? 0,
      });
      this.inFlight -= 1;
      this.lastActivity = Date.now();
    };
    page.on('requestfinished', finish);
    page.on('requestfailed', finish);
  }

  /** Resolve when no GraphQL request has been in flight for `idleMs`; reject after `timeoutMs`. */
  async settled(idleMs: number, timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (this.inFlight === 0 && Date.now() - this.lastActivity >= idleMs) return;
      await new Promise((resolve) => {
        setTimeout(resolve, 100);
      });
    }
    throw new Error(`GraphQL traffic did not settle within ${timeoutMs}ms (${this.inFlight} in flight)`);
  }

  /** Calls aggregated by operation, largest response first. */
  byOperation(): {
    operation: string;
    count: number;
    durationMs: number;
    requestBytes: number;
    responseBytes: number;
  }[] {
    const agg = new Map<
      string,
      { operation: string; count: number; durationMs: number; requestBytes: number; responseBytes: number }
    >();
    for (const c of this.calls) {
      const a = agg.get(c.operation) ?? {
        operation: c.operation,
        count: 0,
        durationMs: 0,
        requestBytes: 0,
        responseBytes: 0,
      };
      a.count += 1;
      a.durationMs += c.durationMs;
      a.requestBytes += c.requestBytes;
      a.responseBytes += c.responseBytes;
      agg.set(c.operation, a);
    }
    return [...agg.values()].sort((x, y) => y.responseBytes - x.responseBytes);
  }
}
