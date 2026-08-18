import { BaseSource } from './BaseSource';

export class CustomSource extends BaseSource {
  async fillForm(): Promise<void> {
    await this.yamlEditor.waitFor({ state: 'visible' });
    await this.yamlEditor.click();
    await this.page.keyboard.press('ControlOrMeta+a');
    await this.page.keyboard.type('source:\n    type: demo-data\nconfig: {}');
  }
}
