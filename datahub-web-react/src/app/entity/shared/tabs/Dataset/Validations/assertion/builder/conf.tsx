import { SelectTypeStep } from './steps/SelectTypeStep';
import { ConfigureDatasetFreshnessAssertionStep } from './steps/ConfigureDatasetFreshnessAssertionStep';
import { ConfigureActionsStep } from './steps/ConfigureActionsStep';

/**
 * Mapping from the step type to the component implementing that step.
 */
export const AssertionsBuilderStepComponent = {
    SELECT_TYPE: SelectTypeStep,
    CONFIGURE_DATASET_FRESHNESS_ASSERTION: ConfigureDatasetFreshnessAssertionStep,
    CONFIGURE_ACTIONS: ConfigureActionsStep,
};

/**
 * Mapping from the step type to the title for the step
 */
export enum AssertionBuilderStepTitles {
    SELECT_TYPE = 'Select Assertion Type',
    CONFIGURE_DATASET_FRESHNESS_ASSERTION = 'Configure Assertion',
    CONFIGURE_ACTIONS = 'Finish up',
}
