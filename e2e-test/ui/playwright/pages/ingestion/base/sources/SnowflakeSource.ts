import { expect, type Locator, type Page } from '@playwright/test';
import type { DataHubLogger } from '@utils/logger';
import { BaseSource } from '@pages/ingestion/base/sources/BaseSource';

export const PRIVATE_KEY_AUTH = 'privateKey' as const;
export const USERNAME_PASSWORD_AUTH = 'userNameAndPassword' as const;
export const DISPLAY_NAME_PRIVATE_KEY = 'Key';
export const DISPLAY_NAME_USERNAME_PASSWORD = 'Username & Password';

const OPTION_TESTID_PRIVATE_KEY = 'option-KEY_PAIR_AUTHENTICATOR';
const OPTION_TESTID_USERNAME_PASSWORD = 'option-DEFAULT_AUTHENTICATOR';

export type SnowflakeAuthType = typeof PRIVATE_KEY_AUTH | typeof USERNAME_PASSWORD_AUTH;

export type SnowflakeFormDetails = {
  accountId?: string;
  warehouseId?: string;
  username?: string;
  password?: string;
  passwordSecret?: string;
  privateKey?: string;
  role?: string;
  authenticationType?: SnowflakeAuthType;
};

export class SnowflakeSource extends BaseSource {
  readonly snowflakeDetailsHeading: Locator;
  readonly accountIdInput: Locator;
  readonly warehouseInput: Locator;
  readonly usernameInput: Locator;
  readonly privateKeyInput: Locator;
  readonly authenticationTypeField: Locator;
  readonly authTypeSelectContainer: Locator;
  readonly passwordInput: Locator;
  readonly roleInput: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.snowflakeDetailsHeading = page.getByText('Snowflake Details');
    // Recipe form fields are rendered with generated ids and carry no data-testid or ARIA role.
    /* eslint-disable playwright/no-raw-locators */
    this.accountIdInput = page.locator('#account_id');
    this.warehouseInput = page.locator('#warehouse');
    this.usernameInput = page.locator('#username');
    this.privateKeyInput = page.locator('#private_key');
    this.authenticationTypeField = page.locator('#authentication_type');
    this.authTypeSelectContainer = page.locator('.ant-select').filter({ has: this.authenticationTypeField });
    this.passwordInput = page.locator('#password');
    this.roleInput = page.locator('#role');
    /* eslint-enable playwright/no-raw-locators */
  }

  async fillForm(details: Partial<SnowflakeFormDetails>): Promise<void> {
    this.logger?.step('fill snowflake details', { details });
    await this.waitForForm();

    await this.fillAccountId(details);
    await this.fillWarehouseId(details);
    await this.fillUsername(details);
    await this.fillAuthenticationType(details);

    if (details.authenticationType === PRIVATE_KEY_AUTH) {
      await this.fillPrivateKey(details);
    } else {
      await this.fillPasswordSecret(details);
      await this.fillPassword(details);
    }

    await this.fillRole(details);
  }

  async waitForForm(): Promise<void> {
    await this.snowflakeDetailsHeading.waitFor({ state: 'visible' });
  }

  async fillAccountId(details: SnowflakeFormDetails): Promise<void> {
    if (details.accountId) {
      await this.fillTextField(this.accountIdInput, details.accountId);
    }
  }

  async fillWarehouseId(details: SnowflakeFormDetails): Promise<void> {
    if (details.warehouseId) {
      await this.fillTextField(this.warehouseInput, details.warehouseId);
    }
  }

  async fillUsername(details: SnowflakeFormDetails): Promise<void> {
    if (details.username) {
      await this.fillTextField(this.usernameInput, details.username);
    }
  }

  async fillPrivateKey(details: SnowflakeFormDetails): Promise<void> {
    if (details.privateKey) {
      await this.fillTextField(this.privateKeyInput, details.privateKey);
    }
  }

  getAuthTypeOption(type: SnowflakeAuthType): Locator {
    const testId = type === PRIVATE_KEY_AUTH ? OPTION_TESTID_PRIVATE_KEY : OPTION_TESTID_USERNAME_PASSWORD;
    return this.getSelectOptionByTestId(testId);
  }

  async fillAuthenticationType(details: SnowflakeFormDetails): Promise<void> {
    if (details.authenticationType) {
      await this.authTypeSelectContainer.scrollIntoViewIfNeeded();
      await this.authTypeSelectContainer.click();
      const option = this.getAuthTypeOption(details.authenticationType);
      await option.scrollIntoViewIfNeeded();
      await option.click();
    }
  }

  async fillPasswordSecret(details: SnowflakeFormDetails): Promise<void> {
    if (details.passwordSecret) {
      await this.fillSecretFieldWithExistingSecret(this.passwordInput, details.passwordSecret);
    }
  }

  async fillPassword(details: SnowflakeFormDetails): Promise<void> {
    if (details.password) {
      await this.fillSecretFieldAsPlainValue(this.passwordInput, details.password);
    }
  }

  async fillRole(details: SnowflakeFormDetails): Promise<void> {
    if (details.role) {
      await this.fillSecretFieldAsPlainValue(this.roleInput, details.role);
    }
  }

  async expectSecretAbsentInPasswordDropdown(secretName: string): Promise<void> {
    this.logger?.step('verify secret absent in password dropdown', { secretName });
    await this.passwordInput.scrollIntoViewIfNeeded();
    await expect(this.secretDropdown).toBeHidden();
    await this.passwordInput.click();
    await this.secretDropdown.waitFor({ state: 'visible' });
    await expect(this.secretDropdown.getByText(secretName, { exact: true })).toBeHidden();
    await this.page.keyboard.press('Escape');
    await this.secretDropdown.waitFor({ state: 'hidden' });
  }

  async expectFormValues(details: Partial<SnowflakeFormDetails>): Promise<void> {
    this.logger?.step('verify snowflake form values', { details });
    await this.waitForForm();

    if (details.accountId) {
      await expect(this.accountIdInput).toHaveValue(details.accountId);
    }
    if (details.warehouseId) {
      await expect(this.warehouseInput).toHaveValue(details.warehouseId);
    }
    if (details.username) {
      await expect(this.usernameInput).toHaveValue(details.username);
    }
    if (details.authenticationType) {
      const displayText =
        details.authenticationType === PRIVATE_KEY_AUTH ? DISPLAY_NAME_PRIVATE_KEY : DISPLAY_NAME_USERNAME_PASSWORD;
      await expect(this.authTypeSelectContainer).toContainText(displayText);
    }
    if (details.password) {
      await expect(this.passwordInput).toHaveValue(details.password);
    }
    if (details.role) {
      await expect(this.roleInput).toHaveValue(details.role);
    }
  }
}
