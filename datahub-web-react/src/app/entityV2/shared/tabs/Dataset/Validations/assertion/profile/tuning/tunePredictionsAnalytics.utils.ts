import { removeNestedTypeNames } from '@app/shared/subscribe/drawer/utils';

import { AssertionAdjustmentSettings } from '@types';

export type TunePredictionsChangedSetting =
    | 'trainingDataLookbackWindowDays'
    | 'sensitivityLevel'
    | 'exclusionWindows'
    | 'algorithm';

type SettingsSummary = {
    trainingDataLookbackWindowDays?: number;
    sensitivityLevel?: number;
    exclusionWindowsFingerprint: string;
    algorithm?: string;
};

const toSettingsSummary = (settings?: AssertionAdjustmentSettings | null): SettingsSummary => {
    const cleaned = (removeNestedTypeNames(settings || {}) || {}) as AssertionAdjustmentSettings;
    const exclusionWindows = cleaned.exclusionWindows || [];
    const exclusionWindowsFingerprint = exclusionWindows.length
        ? exclusionWindows
              .map((window) => JSON.stringify(window))
              .sort()
              .join('|')
        : '';

    return {
        trainingDataLookbackWindowDays: cleaned.trainingDataLookbackWindowDays ?? undefined,
        sensitivityLevel: cleaned.sensitivity?.level ?? undefined,
        exclusionWindowsFingerprint,
        algorithm: (cleaned.algorithmName || cleaned.algorithm) ?? undefined,
    };
};

export const getChangedTunePredictionsSettings = (
    previous: AssertionAdjustmentSettings | undefined,
    next: AssertionAdjustmentSettings,
): TunePredictionsChangedSetting[] => {
    const prev = toSettingsSummary(previous);
    const nxt = toSettingsSummary(next);

    const changed: TunePredictionsChangedSetting[] = [];

    if (prev.trainingDataLookbackWindowDays !== nxt.trainingDataLookbackWindowDays) {
        changed.push('trainingDataLookbackWindowDays');
    }
    if (prev.sensitivityLevel !== nxt.sensitivityLevel) {
        changed.push('sensitivityLevel');
    }
    if (prev.exclusionWindowsFingerprint !== nxt.exclusionWindowsFingerprint) {
        changed.push('exclusionWindows');
    }
    if (prev.algorithm !== nxt.algorithm) {
        changed.push('algorithm');
    }

    return changed;
};
