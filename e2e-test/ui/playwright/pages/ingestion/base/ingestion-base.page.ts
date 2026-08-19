import { BasePage } from '../../base.page';

export const INGESTION_TAB = {
  Sources: 'Sources',
  RunHistory: 'RunHistory',
  Secrets: 'Secrets',
} as const;

export type IngestionTab = (typeof INGESTION_TAB)[keyof typeof INGESTION_TAB];

export abstract class IngestionBasePage extends BasePage {
  async goto(): Promise<void> {
    this.logger?.step('navigate to ingestion page');
    await this.navigate('/ingestion');
    await this.waitForPageLoad();
  }
}
