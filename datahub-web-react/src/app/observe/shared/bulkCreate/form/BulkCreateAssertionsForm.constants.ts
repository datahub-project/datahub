import { DEFAULT_SMART_ASSERTION_SENSITIVITY } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
import { DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { OperatorId } from '@app/tests/builder/steps/definition/builder/property/types/operators';
import { LogicalOperatorType, LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';

import { AssertionActionsInput, CronSchedule, EntityType } from '@types';

export const DEFAULT_ASSET_SELECTOR_FILTERS: LogicalPredicate = {
    type: 'logical',
    operator: LogicalOperatorType.AND,
    operands: [
        {
            type: 'property',
            property: '_entityType',
            operator: OperatorId.EQUAL_TO,
            values: [EntityType.Dataset],
        },
        {
            type: 'property',
            property: 'platform',
            operator: OperatorId.EQUAL_TO,
            values: [],
        },
    ],
};

export const DEFAULT_INFERENCE_SETTINGS: AssertionMonitorBuilderState['inferenceSettings'] = {
    sensitivity: {
        level: DEFAULT_SMART_ASSERTION_SENSITIVITY,
    },
    exclusionWindows: [],
    trainingDataLookbackWindowDays: DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS,
};

export const DEFAULT_CRON_SCHEDULE: CronSchedule = {
    cron: '0 * * * *', // runs every hour at minute 0
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
};

export const DEFAULT_ASSERTION_ACTIONS: AssertionActionsInput = {
    onFailure: [],
    onSuccess: [],
};
