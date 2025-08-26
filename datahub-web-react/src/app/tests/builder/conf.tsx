import { NameStep } from '@app/tests/builder/steps/name/NameStep';
import { RulesStep } from '@app/tests/builder/steps/rules/RulesStep';
import { SelectStep } from '@app/tests/builder/steps/select/SelectStep';

/**
 * Mapping from the step type to the component implementing that step.
 */
export const TestBuilderStepComponent = {
    SELECT: SelectStep,
    RULES: RulesStep,
    NAME: NameStep,
};

/**
 * Mapping from the step type to the title for the step
 */
export enum TestBuilderStepTitles {
    SELECT = 'Select Data Assets',
    RULES = 'Define Conditions',
    // ACTIONS = 'Configure Actions',
    NAME = 'Finish Up',
}
