import { DatasetPlatform } from 'wherehows-web/constants/dataset-platforms';

/**
 * Available values for the purge policy
 * @enum {string}
 */
enum PurgePolicy {
  AutoPurge = 'AUTO_PURGE',
  ManualPurge = 'MANUAL_PURGE',
  AutoLimitedRetention = 'AUTO_LIMITED_RETENTION',
  ManualLimitedRetention = 'MANUAL_LIMITED_RETENTION',
  PurgeExempt = 'PURGE_EXEMPT'
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
    desc: 'A centralized system will automatically purge this dataset based on the provided metadata.',
    displayAs: 'Auto Purge'
  },
  MANUAL_PURGE: {
    platforms: [DatasetPlatform.MySql, DatasetPlatform.Espresso, DatasetPlatform.Teradata, DatasetPlatform.HDFS],
    desc: '',
    displayAs: 'Manual Purge'
  },
  AUTO_LIMITED_RETENTION: {
    platforms: [DatasetPlatform.Kafka, DatasetPlatform.Teradata, DatasetPlatform.HDFS],
    desc:
      'The data platform enforces limited retention. Only choose this option if your dataset ' +
      "complies with the platform's limited retention policy.",
    displayAs: 'Auto Limited Retention'
  },
  MANUAL_LIMITED_RETENTION: {
    platforms: [DatasetPlatform.Espresso, DatasetPlatform.Oracle, DatasetPlatform.MySql],
    desc: '',
    displayAs: 'Manual Limited Retention'
  },
  PURGE_EXEMPT: {
    platforms: Object.keys(DatasetPlatform).map((k: keyof typeof DatasetPlatform) => DatasetPlatform[k]),
    desc:
      'The dataset is exempted from purging due to legal or financial requirements.' +
      " Only choose this option if you've received an explicit exemption from legal.",
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
