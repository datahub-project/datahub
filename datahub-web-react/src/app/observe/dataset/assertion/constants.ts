import { AssertionType } from '@types';

export const FAILING_ASSERTION_TYPE_FILTER_FIELD = 'failingAssertionType';
export const PASSING_ASSERTION_TYPE_FILTER_FIELD = 'passingAssertionType';
export const ERRORED_ASSERTION_TYPE_FILTER_FIELD = 'erroredAssertionType';
export const HAS_FAILING_ASSERTIONS_FILTER_FIELD = 'hasFailingAssertions';
export const HAS_PASSING_ASSERTIONS_FILTER_FIELD = 'hasPassingAssertions';
export const HAS_ERRORED_ASSERTIONS_FILTER_FIELD = 'hasErroredAssertions';

export type AssertionResultTypeOptions = 'Failing' | 'Passing' | 'Error';
export const ASSERTION_RESULT_TYPE_OPTIONS_TO_FILTER_FIELD: Record<AssertionResultTypeOptions, string> = {
    Failing: HAS_FAILING_ASSERTIONS_FILTER_FIELD,
    Passing: HAS_PASSING_ASSERTIONS_FILTER_FIELD,
    Error: HAS_ERRORED_ASSERTIONS_FILTER_FIELD,
};

export const LAST_ASSERTION_RESULT_AT_SORT_FIELD = 'lastAssertionResultAt';

export const ASSERTION_TYPE_OPTIONS = [
    {
        name: 'Freshness',
        value: AssertionType.Freshness,
    },
    {
        name: 'Volume',
        value: AssertionType.Volume,
    },
    {
        name: 'Column',
        value: AssertionType.Field,
    },
    {
        name: 'Custom SQL',
        value: AssertionType.Sql,
    },
    {
        name: 'External',
        value: AssertionType.Dataset,
    },
];

export const NAME_TO_VALUE = new Map();
ASSERTION_TYPE_OPTIONS.forEach((option) => NAME_TO_VALUE.set(option.name, option.value));

export const TYPE_TO_DISPLAY_NAME = new Map();
ASSERTION_TYPE_OPTIONS.forEach((option) => TYPE_TO_DISPLAY_NAME.set(option.value, option.name));
