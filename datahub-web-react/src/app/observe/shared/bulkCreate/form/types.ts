import {
    AssertionMonitorBuilderState,
    FreshnessAssertionBuilderScheduleType,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

import {
    AssertionActionsInput,
    CronSchedule,
    DatasetFreshnessSourceType,
    DatasetVolumeSourceType,
    DateInterval,
} from '@types';

export type FreshnessFormState = {
    freshnessAssertionEnabled: boolean;
    freshnessAssertionType: FreshnessAssertionBuilderScheduleType;
    freshnessAssertionInterval: number;
    freshnessAssertionIntervalUnit: DateInterval;
    freshnessInferenceSettings: AssertionMonitorBuilderState['inferenceSettings'];
    freshnessSchedule: CronSchedule | undefined;
    freshnessActions: AssertionActionsInput;
    freshnessSourceType: DatasetFreshnessSourceType;
};

export type VolumeFormState = {
    volumeAssertionEnabled: boolean;
    volumeInferenceSettings: AssertionMonitorBuilderState['inferenceSettings'];
    volumeSchedule: CronSchedule | undefined;
    volumeActions: AssertionActionsInput;
    volumeSourceType: DatasetVolumeSourceType;
};
