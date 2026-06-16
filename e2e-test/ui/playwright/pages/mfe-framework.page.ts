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
  readonly mfeContainer = () => this.page.getByTestId('mfe-configurable-container');
  readonly mfeItemByName = (name: string) => this.page.getByText(name);
  readonly errorMessage = (mfeName: string) => this.page.getByText(`${mfeName} is not available`);

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
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
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
}
