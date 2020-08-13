/**
 * Available values for the purge policy
 * @enum {string}
 */
export enum PurgePolicy {
  AutoPurge = 'AUTO_PURGE',
  ManualPurge = 'MANUAL_PURGE',
  AutoLimitedRetention = 'LIMITED_RETENTION',
  AutoLimitedWithLocking = 'LIMITED_RETENTION_WITH_LOCKING',
  ManualLimitedRetention = 'MANUAL_LIMITED_RETENTION',
  PurgeExempt = 'PURGE_EXEMPTED',
  NotApplicable = 'PURGE_NOT_APPLICABLE'
}
