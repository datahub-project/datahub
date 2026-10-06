import i18next from 'i18next';

// Lazy getters: these tab names double as URL route segments (see AcrylValidationsTab/useGetValidationsTab),
// so the display label and the route comparison must resolve to the same translated value. Evaluating at
// call time (not import time) ensures i18n is initialized before the string is produced.
export const getGovernanceTabName = (): string => i18next.t('entity.types:dataset.governanceTab');
export const getQualityTabName = (): string => i18next.t('entity.types:dataset.qualityTab');
