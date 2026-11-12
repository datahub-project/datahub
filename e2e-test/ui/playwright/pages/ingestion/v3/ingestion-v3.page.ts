import { type Page } from '@playwright/test';
import { IngestionBasePage } from '../base/ingestion-base.page';
import { SourcesV3Tab } from './tabs/sources-v3.tab';
import type { DataHubLogger } from '../../../utils/logger';
import { RunHistoryV3Tab } from './tabs/run-history-v3.tab';
import { SecretsTabV3 } from './tabs/secrets-v3.tab';

export {
  type SnowflakeSourceDetails,
  type ScheduleOptions,
  type CreateIngestionSourceOptions,
  type SnowflakeIngestionSourceOptions,
} from '../base/tabs/sources-base.tab';

export class IngestionV3Page extends IngestionBasePage {
  readonly sourcesTab: SourcesV3Tab;
  readonly runHistoryTab: RunHistoryV3Tab;
  readonly secretsTab: SecretsTabV3;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.sourcesTab = new SourcesV3Tab(page, logger, logDir);
    this.runHistoryTab = new RunHistoryV3Tab(page, logger, logDir);
    this.secretsTab = new SecretsTabV3(page, logger, logDir);
  }
}
