import { SelectTypeStep } from './steps/SelectTypeStep';
import { ConfigureDatasetFreshnessAssertionStep } from './steps/ConfigureDatasetFreshnessAssertionStep';
import { ConfigureActionsStep } from './steps/ConfigureActionsStep';
import { ConfigureDatasetVolumeAssertionStep } from './steps/ConfigureDatasetVolumeAssertionStep';
import { ConfigureDatasetSqlAssertionStep } from './steps/ConfigureDatasetSqlAssertionStep';
import { AssertionType } from '../../../../../../../../types.generated';
import { AssertionBuilderStep } from './types';
import { ConfigureDatasetFieldAssertionStep } from './steps/ConfigureDatasetFieldAssertionStep';

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
                default:
                    throw new Error(`Unsupported assertion type ${type}`);
            }
        case AssertionBuilderStep.CONFIGURE_ACTIONS:
            return ConfigureActionsStep;
        default:
            throw new Error(`Unsupported assertion builder step ${currentStep}`);
    }
};

/**
 * Mapping from the step type to the title for the step
 */
export enum AssertionBuilderStepTitles {
    SELECT_TYPE = 'Select Assertion Type',
    CONFIGURE_ASSERTION = 'Configure Assertion',
    CONFIGURE_ACTIONS = 'Finish up',
}
