import { expect, Page, Locator } from '@playwright/test';
import {
  SnowflakeFormDetails,
  SnowflakeSource,
  PRIVATE_KEY_AUTH,
  DISPLAY_NAME_PRIVATE_KEY,
  DISPLAY_NAME_USERNAME_PASSWORD,
} from '@pages/ingestion/base/sources/SnowflakeSource';
import { DataHubLogger } from '@utils/logger';

export class SnowflakeSourceV3 extends SnowflakeSource {
  readonly yamlSwitcherButton: Locator;
  readonly snowflakeConnectionDetailsHeading: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.yamlSwitcherButton = page.getByTestId('yaml-editor-tab');
    this.snowflakeConnectionDetailsHeading = page.getByText('Snowflake Connection Details');
  }

  getYamlSwitcherButton(): Locator {
    return this.yamlSwitcherButton;
  }

  getAuthTypeFormItem(): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- recipe form field addressed by generated id; no data-testid
    return this.page.locator('#authentication_type');
  }

  async waitForForm(): Promise<void> {
    await this.snowflakeConnectionDetailsHeading.waitFor({ state: 'visible' });
  }

  // Scroll to role (below auth type), then username (above) to bring the
  // select into view — the Ant select is not added to DOM when off-screen.
  private async scrollAuthTypeIntoView(): Promise<void> {
    await this.roleInput.scrollIntoViewIfNeeded();
    await this.usernameInput.scrollIntoViewIfNeeded();
  }

  override async expectFormValues(details: Partial<SnowflakeFormDetails>): Promise<void> {
    const { authenticationType, ...otherDetails } = details;
    // Check all fields except authenticationType first so prior checks don't scroll it off-screen
    await super.expectFormValues(otherDetails);

    if (authenticationType) {
      await this.scrollAuthTypeIntoView();
      const displayText =
        authenticationType === PRIVATE_KEY_AUTH ? DISPLAY_NAME_PRIVATE_KEY : DISPLAY_NAME_USERNAME_PASSWORD;
      await expect(this.getAuthTypeFormItem()).toContainText(displayText);
    }
  }

  async fillAuthenticationType(details: SnowflakeFormDetails): Promise<void> {
    if (details.authenticationType) {
      await this.scrollAuthTypeIntoView();
      await this.authenticationTypeField.scrollIntoViewIfNeeded();
      await this.authenticationTypeField.click();
      await this.getAuthTypeOption(details.authenticationType).click();
    }
  }
}
