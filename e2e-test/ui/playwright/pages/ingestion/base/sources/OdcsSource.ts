import { BaseSource } from './BaseSource';

export class OdcsSource extends BaseSource {
  async fillPath(path: string): Promise<void> {
    this.logger?.step('fill odcs path', { path });
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Form.Item renders the input id from the field name; no data-testid on text fields
    const pathInput = this.page.locator('#path');
    await this.fillTextField(pathInput, path);
  }
}
