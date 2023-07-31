import { AssertionType } from '../../../../types.generated';

export const FAILING_ASSERTION_TYPE_FILTER_FIELD = 'failAssertionType';
export const HAS_FAILING_ASSERTIONS_FILTER_FIELD = 'hasFailingAssertions';

export const ASSERTION_TYPE_OPTIONS = [
    {
        name: 'Freshness',
        value: AssertionType.Freshness,
    },
    {
        name: 'Other',
        value: AssertionType.Dataset,
    },
];

export const NAME_TO_VALUE = new Map();
ASSERTION_TYPE_OPTIONS.forEach((option) => NAME_TO_VALUE.set(option.name, option.value));

export const TYPE_TO_DISPLAY_NAME = new Map();
ASSERTION_TYPE_OPTIONS.forEach((option) => TYPE_TO_DISPLAY_NAME.set(option.value, option.name));
