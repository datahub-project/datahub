import { expect, type Locator, type Page } from '@playwright/test';
import type { DataHubLogger } from '../../../../utils/logger';

export class BaseSource {
  readonly recipeBuilderYamlButton: Locator;
  readonly toggleExpandButton: Locator;
  readonly yamlEditor: Locator;
  readonly secretDropdown: Locator;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    this.recipeBuilderYamlButton = page.getByTestId('recipe-builder-yaml-button');
    this.toggleExpandButton = page.getByTestId('toggle-expand-button');
    // eslint-disable-next-line playwright/no-raw-locators -- Monaco editor internal scroll container; no data-testid or ARIA role
    this.yamlEditor = page.locator('.monaco-scrollable-element').and(page.locator(':visible'));
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design select dropdown portal; no data-testid or ARIA role
    this.secretDropdown = page.locator('.ant-select-dropdown').and(page.locator(':visible'));
  }

  async fillTextField(locator: Locator, value: string): Promise<void> {
    await locator.scrollIntoViewIfNeeded();
    await locator.clear();
    await locator.fill(value);
  }

  async fillSecretFieldAsPlainValue(locator: Locator, value: string): Promise<void> {
    await this.fillTextField(locator, value);
  }

  getSelectOptionByTestId(testId: string): Locator {
    return this.page.getByTestId(testId);
  }

  async fillSecretFieldWithExistingSecret(locator: Locator, value: string): Promise<void> {
    await expect(this.secretDropdown).toBeHidden();
    await locator.scrollIntoViewIfNeeded();
    await locator.fill(value);
    await this.secretDropdown.waitFor({ state: 'visible' });
    await this.secretDropdown.getByText(value, { exact: true }).click();
    if (await this.secretDropdown.isVisible()) {
      // Use Tab rather than Escape to close the dropdown. Ant Design modals
      // with keyboard:true close on Escape, which would dismiss the entire
      // wizard if the keydown event propagates up from the AutoComplete.
      await this.page.keyboard.press('Tab');
    }
    await expect(this.secretDropdown).toBeHidden();
  }

  getYamlSwitcherButton(): Locator {
    return this.recipeBuilderYamlButton;
  }

  async expectYamlRecipe(values: string[]): Promise<void> {
    this.logger?.step('verify yaml recipe');
    const yamlSwitcherButton = this.getYamlSwitcherButton();
    const hasYamlButton = await yamlSwitcherButton.isVisible();
    if (hasYamlButton) {
      await yamlSwitcherButton.scrollIntoViewIfNeeded();
      await yamlSwitcherButton.click();
    }

    const isExpandButtonVisible = await this.toggleExpandButton.isVisible();
    if (isExpandButtonVisible) {
      await this.toggleExpandButton.click();
    }

    await Promise.all(
      values.map(async (value) => {
        await expect(this.yamlEditor).toContainText(value);
      }),
    );
  }
}
