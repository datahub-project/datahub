import { test, expect } from '../../fixtures/base-test';
import { MFEFrameworkPage } from '../../pages/mfe-framework.page';
import { TIMEOUTS } from '../../utils/constants';

const MFE_CONFIG_YAML = `subNavigationMode: false
microFrontends:
  - id: HelloWorld
    label: HelloWorld Cypress
    path: /helloworld-mfe
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: helloWorldMFE/mount
    flags:
      enabled: true
      showInNav: true
    navIcon: HandWaving`;

const REMOTE_ENTRY_SUCCESS = `
window.helloWorldMFE = {
  init: function(shared) {
    console.log('helloWorldMFE.init() called');
  },
  get: function(module) {
    console.log('helloWorldMFE.get() called for:', module);
    return Promise.resolve(() => {
      return function mount(containerElement, props) {
        if (containerElement) {
          containerElement.innerHTML = '<h1>HelloWorld Cypress MFE Mounted</h1>';
        }
        return () => console.log('cleanup called');
      };
    });
  }
};`;

const MFE_NAME = 'HelloWorld Cypress';
const MFE_PATH = '/helloworld-mfe';
const MFE_CONTENT = 'HelloWorld Cypress MFE Mounted';

test.describe('MFE Framework Configuration', () => {
  test.beforeEach(async ({ page }) => {
    const mfePage = new MFEFrameworkPage(page);
    await mfePage.setupMFEFramework(MFE_CONFIG_YAML);
  });

  test('shows all MFE items from YAML in the sidebar', async ({ page }) => {
    const mfePage = new MFEFrameworkPage(page);

    await mfePage.waitForSidebar();
    await mfePage.waitForPageLoad();
    await expect(mfePage.mfeItemByName(MFE_NAME)).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('navigates to MFE route when sidebar item is clicked and shows error if mounting fails', async ({ page }) => {
    const mfePage = new MFEFrameworkPage(page);

    await mfePage.mockRemoteEntry(503, 'Service Unavailable');
    await mfePage.waitForSidebar();
    await mfePage.clickMFEItem(MFE_NAME);
    await mfePage.waitForMFENavigation(MFE_PATH);
    await expect(mfePage.errorMessage(MFE_NAME)).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('navigates to MFE route when sidebar item is clicked and shows MFE container on success', async ({ page }) => {
    const mfePage = new MFEFrameworkPage(page);

    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_SUCCESS);
    await mfePage.waitForSidebar();
    await mfePage.clickMFEItem(MFE_NAME);
    await mfePage.waitForMFENavigation(MFE_PATH);
    await expect(mfePage.mfeContainer()).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(mfePage.mfeContainer().getByText(MFE_CONTENT)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });
});
