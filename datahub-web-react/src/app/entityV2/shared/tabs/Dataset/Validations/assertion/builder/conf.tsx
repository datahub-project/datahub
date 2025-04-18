import { ConfigureDatasetFieldAssertionStep } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/ConfigureDatasetFieldAssertionStep';
import { ConfigureDatasetFreshnessAssertionStep } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/ConfigureDatasetFreshnessAssertionStep';
import { ConfigureDatasetSchemaAssertionStep } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/ConfigureDatasetSchemaAssertionStep';
import { ConfigureDatasetSqlAssertionStep } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/ConfigureDatasetSqlAssertionStep';
import { ConfigureDatasetVolumeAssertionStep } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/ConfigureDatasetVolumeAssertionStep';
import { FinishUpStep } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/FinishUpStep';
import { SelectTypeStep } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/SelectTypeStep';
import { AssertionBuilderStep } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionType } from '@types';

/**
 * Mapping from the step type to the component implementing that step.
 */
export const getAssertionsBuilderStepComponent = (currentStep: AssertionBuilderStep, type?: AssertionType) => {
    switch (currentStep) {
        case AssertionBuilderStep.SELECT_TYPE:
            return SelectTypeStep;
        case AssertionBuilderStep.CONFIGURE_ASSERTION:
            switch (type) {
                case AssertionType.Freshness:
                    return ConfigureDatasetFreshnessAssertionStep;
                case AssertionType.Volume:
                    return ConfigureDatasetVolumeAssertionStep;
                case AssertionType.Sql:
                    return ConfigureDatasetSqlAssertionStep;
                case AssertionType.Field:
                    return ConfigureDatasetFieldAssertionStep;
                case AssertionType.DataSchema:
                    return ConfigureDatasetSchemaAssertionStep;
                default:
                    throw new Error(`Unsupported assertion type ${type}`);
            }
        case AssertionBuilderStep.FINISH_UP:
            return FinishUpStep;
        default:
            throw new Error(`Unsupported assertion builder step ${currentStep}`);
    }
};

/**
 * Mapping from the step type to the title for the step
 */
export enum AssertionBuilderStepTitles {
    SELECT_TYPE = 'Choose Type',
    CONFIGURE_ASSERTION = 'Configure',
    CONFIGURE_ACTIONS = 'Finish up',
    FINISH_UP = 'Finish up',
}
