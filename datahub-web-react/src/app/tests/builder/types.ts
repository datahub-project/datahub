import { DEFAULT_TEST_CATEGORY } from '../constants';

export const DEFAULT_TEST_DEFINITION = {
    on: {
        types: [],
    },
    rules: [],
};

export const DEFAULT_BUILDER_STATE = {
    category: DEFAULT_TEST_CATEGORY,
    description: null,
    definition: {
        json: JSON.stringify(DEFAULT_TEST_DEFINITION),
    },
};

/**
 * The object represents the state of the Test Builder form.
 */
export interface TestBuilderState {
    /**
     * The name of the test.
     */
    name?: string;

    /**
     * The category of the test.
     */
    category?: string;

    /**
     * An optional description for the test.
     */
    description?: string | null;

    /**
     * The definition of the test
     */
    definition?: {
        /**
         * The JSON version of a test definition.
         */
        json?: string | null;
    };
}

/**
 * Enabled steps in the builder flow.
 */
export enum TestBuilderStep {
    SELECT = 'SELECT',
    RULES = 'RULES',
    NAME = 'NAME',
}

/**
 * Props provided to each step as input.
 */
export type StepProps = {
    state: TestBuilderState;
    updateState: (newState: TestBuilderState) => void;
    goTo: (step: TestBuilderStep) => void;
    prev?: () => void;
    submit: (shouldRun?: boolean) => void;
    cancel: () => void;
};
