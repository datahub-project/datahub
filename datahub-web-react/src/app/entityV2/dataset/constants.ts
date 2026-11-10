import i18next from 'i18next';

// Lazy getters: evaluating at call time (not import time) ensures i18n is initialized before the label is produced.
// These are display labels only; the tabs are routed by `EntityTabPath.QUALITY` / `EntityTabPath.GOVERNANCE`.
export const getGovernanceTabName = (): string => i18next.t('entity.types:dataset.governanceTab');
export const getQualityTabName = (): string => i18next.t('entity.types:dataset.qualityTab');
