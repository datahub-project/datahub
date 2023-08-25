import { SelectTypeStep } from './steps/SelectTypeStep';
import { ConfigureDatasetFreshnessAssertionStep } from './steps/ConfigureDatasetFreshnessAssertionStep';
import { ConfigureActionsStep } from './steps/ConfigureActionsStep';
import { ConfigureDatasetVolumeAssertionStep } from './steps/ConfigureDatasetVolumeAssertionStep';
import { AssertionType } from '../../../../../../../../types.generated';
import { AssertionBuilderStep } from './types';

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
