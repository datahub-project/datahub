import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';

/**
 * Available values for the purge policy
 * @enum {string}
 */
enum PurgePolicy {
  AutoPurge = 'AUTO_PURGE',
  ManualPurge = 'MANUAL_PURGE',
  AutoLimitedRetention = 'LIMITED_RETENTION',
  AutoLimitedWithLocking = 'LIMITED_RETENTION_WITH_LOCKING',
  ManualLimitedRetention = 'MANUAL_LIMITED_RETENTION',
  PurgeExempt = 'PURGE_EXEMPTED',
  NotApplicable = 'PURGE_NOT_APPLICABLE'
}

/**
 * Index signature for purge policy properties
 */
type PurgePolicyProperties = {
  [K in PurgePolicy]: {
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
    desc:
      'Choose this option only if it’s acceptable to have the centralized system purge this dataset based on the provided metadata (e.g. member ID, seat ID etc).',
    displayAs: 'Auto Purge'
  },
  MANUAL_PURGE: {
    desc: 'Choose this option only if you or your team have implemented a custom mechanism to purge this dataset.',
    displayAs: 'Manual Purge'
  },
  LIMITED_RETENTION: {
    desc:
      'Choose this option only if you rely on the data platform’s default limited retention mechanism to purge your data.',
    displayAs: 'Auto Limited Retention Without Locking'
  },
  LIMITED_RETENTION_WITH_LOCKING: {
    desc:
      'Choose this option only if you want to rely on the data platform’s default mechanism to lockdown, prior to removal, the dataset when it is older than the predefined limit.',
    displayAs: 'Auto Limited Retention With Locking'
  },
  MANUAL_LIMITED_RETENTION: {
    desc: 'Choose this option only if you have a well established process to ensure limited data retention.',
    displayAs: 'Manual Limited Retention'
  },
  PURGE_EXEMPTED: {
    desc: 'Choose this option only if the dataset is explicitly exempted from purging.',
    displayAs: 'Purge Exempt'
  },
  PURGE_NOT_APPLICABLE: {
    desc: 'Choose this option only if none of the purge policies are applicable.',
    displayAs: 'Purge Not Applicable'
  }
};

/**
 * Extracts the purge policy for a given platform from the list of DatasetPlatforms
 * @param {IDataPlatform.name} platformName the name of the dataset platform
 * @param {Array<IDataPlatform>} [platforms=[]] the list of objects with IDataPlatform interface
 * @returns {Array<PurgePolicy>}
 */
const getSupportedPurgePolicies = (
  platformName: IDataPlatform['name'],
  platforms: Array<IDataPlatform> = []
): Array<PurgePolicy> => {
  const platform = platforms.findBy('name', platformName);
  return platform ? platform.supportedPurgePolicies : [];
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

export { PurgePolicy, purgePolicyProps, isExempt, exemptPolicy, getSupportedPurgePolicies };
