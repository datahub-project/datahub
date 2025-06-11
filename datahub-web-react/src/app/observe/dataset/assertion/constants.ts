import { AssertionType } from '@types';

export type AssertionResultTypeOptions = 'Failing' | 'Passing' | 'Error';

// Dataset level
export const FAILING_ASSERTION_TYPE_FILTER_FIELD = 'failingAssertionType';
export const PASSING_ASSERTION_TYPE_FILTER_FIELD = 'passingAssertionType';
export const ERRORED_ASSERTION_TYPE_FILTER_FIELD = 'erroredAssertionType';
export const HAS_FAILING_ASSERTIONS_FILTER_FIELD = 'hasFailingAssertions';
export const HAS_PASSING_ASSERTIONS_FILTER_FIELD = 'hasPassingAssertions';
export const HAS_ERRORED_ASSERTIONS_FILTER_FIELD = 'hasErroredAssertions';
export const ASSERTION_RESULT_TYPE_OPTIONS_TO_FILTER_FIELD: Record<AssertionResultTypeOptions, string> = {
    Failing: HAS_FAILING_ASSERTIONS_FILTER_FIELD,
    Passing: HAS_PASSING_ASSERTIONS_FILTER_FIELD,
    Error: HAS_ERRORED_ASSERTIONS_FILTER_FIELD,
};

// Assertion level
export const LAST_ASSERTION_FAILED_AT_FILTER_FIELD = 'lastFailedAtMillis';
export const LAST_ASSERTION_ERRORED_AT_FILTER_FIELD = 'lastErroredAtMillis';
export const LAST_ASSERTION_PASSED_AT_FILTER_FIELD = 'lastPassedAtMillis';
export const ASSERTION_RESULT_TYPE_OPTIONS_TO_RUN_SUMMARY_FILTER_FIELD: Record<AssertionResultTypeOptions, string> = {
    Failing: LAST_ASSERTION_FAILED_AT_FILTER_FIELD,
    Passing: LAST_ASSERTION_PASSED_AT_FILTER_FIELD,
    Error: LAST_ASSERTION_ERRORED_AT_FILTER_FIELD,
};

export const ASSERTIONS_DOCS_LINK = 'https://docs.datahub.com/docs/managed-datahub/observe/assertions';

// dataset level
export const LAST_ASSERTION_RESULT_AT_SORT_FIELD = 'lastAssertionResultAt';

// assertion level
export const LAST_ASSERTION_RUN_AT_SORT_FIELD = 'lastCompletedTime';

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

export const RUN_EVENTS_PREVIEW_LIMIT = 1000;

export const ASSERTION_TYPE_FILTER_OPTIONS: AssertionType[] = [
    AssertionType.Freshness,
    AssertionType.Volume,
    AssertionType.Field,
    AssertionType.Sql,
    AssertionType.DataSchema,
    AssertionType.Dataset,
    AssertionType.Custom,
];
