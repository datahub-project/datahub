import { DatasetPlatform } from 'wherehows-web/constants/dataset-platforms';

/**
 * Available values for the purge policy
 * @enum {string}
 */
enum PurgePolicy {
  AutoPurge = 'AUTO_PURGE',
  ManualPurge = 'MANUAL_PURGE',
  AutoLimitedRetention = 'LIMITED_RETENTION',
  ManualLimitedRetention = 'MANUAL_LIMITED_RETENTION',
  PurgeExempt = 'PURGE_EXEMPTED'
}

/**
 * Index signature for purge policy properties
 */
type PurgePolicyProperties = {
  [K in PurgePolicy]: {
    platforms: Array<DatasetPlatform>;
    desc: string;
    displayAs: string;
  }
};

/**
 * Client options for each purge policy
 * Lists, the available platforms, a descriptions field and a user friendly name for the purge key
 */
const purgePolicyProps: PurgePolicyProperties = {
  AUTO_PURGE: {
    platforms: [DatasetPlatform.Teradata, DatasetPlatform.Espresso, DatasetPlatform.HDFS],
    desc:
      'Choose this option only if it’s acceptable to have the centralized system purge this dataset based on the provided metadata (e.g. member ID, seat ID etc).',
    displayAs: 'Auto Purge'
  },
  MANUAL_PURGE: {
    platforms: [DatasetPlatform.MySql, DatasetPlatform.Espresso, DatasetPlatform.Teradata, DatasetPlatform.Oracle, DatasetPlatform.HDFS],
    desc: 'Choose this option only if you or your team have implemented a custom mechanism to purge this dataset.',
    displayAs: 'Manual Purge'
  },
  LIMITED_RETENTION: {
    platforms: [DatasetPlatform.Kafka, DatasetPlatform.Teradata, DatasetPlatform.HDFS],
    desc:
      'Choose this option only if you rely on the data platform’s default limited retention mechanism to purge your data.',
    displayAs: 'Auto Limited Retention'
  },
  MANUAL_LIMITED_RETENTION: {
    platforms: [DatasetPlatform.Espresso, DatasetPlatform.Oracle, DatasetPlatform.MySql],
    desc: 'Choose this option only if you have a well established process to ensure limited data retention.',
    displayAs: 'Manual Limited Retention'
  },
  PURGE_EXEMPTED: {
    platforms: Object.keys(DatasetPlatform).map((k: keyof typeof DatasetPlatform) => DatasetPlatform[k]),
    desc: 'Choose this option only if the dataset is explicitly exempted from purging',
    displayAs: 'Purge Exempt'
  }
};

/**
 * A cache for the exempt policy
 * @type {PurgePolicy}
 */
const exemptPolicy = PurgePolicy.PurgeExempt;

/**
 * Checks that a purge policy is exempt
 * @param {PurgePolicy} policy the policy to check
 */
const isExempt = (policy: PurgePolicy) => policy === PurgePolicy.PurgeExempt;

export { PurgePolicy, purgePolicyProps, isExempt, exemptPolicy };
