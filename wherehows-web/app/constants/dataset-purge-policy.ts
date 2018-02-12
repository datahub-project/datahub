import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';

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
    displayAs: 'Auto Limited Retention'
  },
  MANUAL_LIMITED_RETENTION: {
    desc: 'Choose this option only if you have a well established process to ensure limited data retention.',
    displayAs: 'Manual Limited Retention'
  },
  PURGE_EXEMPTED: {
    desc: 'Choose this option only if the dataset is explicitly exempted from purging',
    displayAs: 'Purge Exempt'
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

/**
 * User informational text for datasets without a purge policy
 * @type {string}
 */
const missingPolicyText = 'This dataset does not have a current compliance purge policy.';

export { PurgePolicy, purgePolicyProps, isExempt, exemptPolicy, missingPolicyText, getSupportedPurgePolicies };
