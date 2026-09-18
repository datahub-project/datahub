import { Page } from '@playwright/test';
import { TIMEOUTS, LOAD_STATES } from '../utils/constants';

/**
 * MFE Framework page object — encapsulates all MFE-related selectors and interactions
 *
 * Handles:
 * - Navigation to MFE sidebar items
 * - MFE configuration mocking
 * - Remote entry response mocking
 * - MFE content verification
 */
export class MFEFrameworkPage {
  readonly page: Page;

  // Selectors
  readonly navBarItemHome = () => this.page.getByTestId('nav-bar-item-home');
  readonly navSidebar = () => this.page.getByTestId('nav-sidebar');
  readonly mfeContainer = () => this.page.getByTestId('mfe-configurable-container');
  readonly mfeItemByName = (name: string) => this.page.getByText(name);
  readonly errorMessage = (mfeName: string) => this.page.getByText(`${mfeName} is not available`);

  // Slot (entity.detail.tab) selectors
  readonly entityHeader = () => this.page.getByTestId('entity-header-test-id');
  readonly entityTabHeader = (tabName: string) => this.page.getByTestId(`${tabName}-entity-tab-header`);
  readonly slotContainer = () => this.page.getByTestId('mfe-slot-container');
  readonly slotCtx = () => this.page.getByTestId('mfe-slot-ctx');
  readonly notFoundPage = () => this.page.getByRole('button', { name: 'Back to Home' });

  constructor(page: Page) {
    this.page = page;
  }

  /**
   * Mock fetch requests at browser level to intercept /mfe/config
   * Must be called BEFORE navigation
   */
  async mockFetchForMFEConfig(yamlConfig: string): Promise<void> {
    await this.page.addInitScript((configYaml: string) => {
      const originalFetch = window.fetch;
      window.fetch = ((resource: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
        let url = '';
        if (typeof resource === 'string') {
          url = resource;
        } else if (resource instanceof URL) {
          url = resource.toString();
        } else {
          url = resource.url;
        }
        if (url.includes('/mfe/config')) {
          return Promise.resolve(
            new Response(configYaml, {
              status: 200,
              headers: { 'Content-Type': 'text/plain' },
            }),
          );
        }
        return originalFetch(resource, init);
      }) as typeof window.fetch;
    }, yamlConfig);
  }

  /**
   * Setup mock response for remote entry (success or failure)
   */
  async mockRemoteEntry(status: number, body: string): Promise<void> {
    await this.page.route('**/remoteEntry.js', async (route) => {
      await route.fulfill({
        status,
        contentType: status === 200 ? 'application/javascript' : 'text/plain',
        body,
      });
    });
  }

  /**
   * Navigate to home page and setup initial state
   */
  async navigateToHome(): Promise<void> {
    await this.page.goto('/');
  }

  /**
   * Skip intro page by setting localStorage
   */
  async skipIntroPage(): Promise<void> {
    await this.page.evaluate(() => {
      localStorage.setItem('skipAcrylIntroducePage', 'true');
    });
  }

  /**
   * Wait for initial page load and MFE config to be fetched
   */
  async waitForPageLoad(): Promise<void> {
    await this.page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    await this.waitForSidebar();
  }

  /**
   * Wait for sidebar navigation to be visible
   */
  async waitForSidebar(): Promise<void> {
    await this.navBarItemHome().waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  /**
   * Wait for MFE item to appear in sidebar
   */
  async waitForMFEItem(mfeName: string): Promise<void> {
    await this.mfeItemByName(mfeName).waitFor({
      state: 'visible',
      timeout: TIMEOUTS.LONG,
    });
  }

  /**
   * Click on MFE item in sidebar to navigate
   */
  async clickMFEItem(mfeName: string): Promise<void> {
    await this.mfeItemByName(mfeName).click();
  }

  /**
   * Wait for navigation to MFE route
   */
  async waitForMFENavigation(mfePath: string): Promise<void> {
    await this.page.waitForURL(`**${mfePath}`);
  }

  /**
   * Complete setup flow: mock config, navigate, wait for load
   */
  async setupMFEFramework(yamlConfig: string): Promise<void> {
    await this.mockFetchForMFEConfig(yamlConfig);
    await this.navigateToHome();
    await this.skipIntroPage();
    await this.waitForPageLoad();
  }

  // ── Slot (entity.detail.tab) helpers ──────────────────────────────────────

  /**
   * Start counting requests for the remote entry bundle. Must be called BEFORE navigation.
   * Returns a getter so tests can assert lazy-loading behaviour (0 until the tab is opened).
   */
  trackRemoteEntryRequests(): () => number {
    let count = 0;
    this.page.on('request', (request) => {
      if (request.url().includes('remoteEntry.js')) count += 1;
    });
    return () => count;
  }

  /**
   * Navigate straight to an entity profile page, optionally deep-linking to a tab.
   * Mocks for /mfe/config and remoteEntry.js must already be installed.
   */
  async gotoDataset(urn: string, tabName?: string): Promise<void> {
    const tabSegment = tabName ? `/${encodeURIComponent(tabName)}` : '';
    await this.page.goto(`/dataset/${encodeURIComponent(urn)}${tabSegment}`);
    await this.page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    // The first profile load against a Vite dev server compiles the entity page on demand.
    await this.entityHeader().waitFor({ state: 'visible', timeout: TIMEOUTS.EXTRA_LONG * 2 });
  }

  /**
   * Parse the context JSON the stub remote rendered into the slot container.
   */
  async readSlotCtx(): Promise<Record<string, unknown>> {
    await this.slotCtx().waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    const text = await this.slotCtx().textContent();
    return JSON.parse(text ?? '{}') as Record<string, unknown>;
  }
}
