import { removeEmptyArrays } from '../utils';

const EMPTY_AND_CONDITONS = {
    on: {
        types: 'dataset',
        conditions: {
            and: [],
        },
    },
    rules: [],
};
const EMPTY_OR_CONDITONS = {
    on: {
        types: 'dataset',
        conditions: {
            or: [],
        },
    },
    rules: [],
};
const EMPTY_NOT_CONDITONS = {
    on: {
        types: 'dataset',
        conditions: {
            not: [],
        },
    },
    rules: [],
};

const DEAFULT_EMPTY_CONDITIONS_RESULT = {
    on: {
        types: 'dataset',
    },
    rules: [],
};

const PROPERTY_CONDITIONS = {
    on: {
        types: [],
        conditions: {
            and: {
                property: 'test',
                operator: 'exists',
            },
        },
    },
    rules: [],
};

const NULL_CONDITIONS = {
    source: {
        type: 'snowflake',
        config: {
            profiling: {
                profile_table_size_limit: null,
            },
        },
    },
};

const COMPLEX_CONDITIONS = {
    on: {
        types: ['dataset', 'dashboard'],
        conditions: {
            and: [
                { property: 'urn', operator: 'equals', values: ['sample', 'cypress'] },
                {
                    or: [
                        { property: 'entityType', operator: 'exists' },
                        { and: [{ not: [{ property: 'dataPlatformInstance.platform', operator: 'exists' }] }] },
                    ],
                },
                {
                    not: [
                        {
                            and: [
                                {
                                    property: 'ownership.owners.owner',
                                    operator: 'contains_any',
                                    values: ['urn:li:corpGroup:bfoo', 'urn:li:corpGroup:jdoe'],
                                },
                            ],
                        },
                        {
                            or: [
                                {
                                    property: 'domains.domains',
                                    operator: 'equals',
                                    values: ['urn:li:domain:marketing'],
                                },
                            ],
                        },
                    ],
                },
            ],
        },
    },
    rules: [],
};
describe('removeEmptyArrays', () => {
    test('removes empty AND condition array from a nested object', () => {
        const result = removeEmptyArrays(EMPTY_AND_CONDITONS);
        expect(result).toEqual(DEAFULT_EMPTY_CONDITIONS_RESULT);
    });

    test('removes empty OR condition array from a nested object', () => {
        const result = removeEmptyArrays(EMPTY_OR_CONDITONS);
        expect(result).toEqual(DEAFULT_EMPTY_CONDITIONS_RESULT);
    });

    test('removes empty NOT COndition array from a nested object', () => {
        const result = removeEmptyArrays(EMPTY_NOT_CONDITONS);
        expect(result).toEqual(DEAFULT_EMPTY_CONDITIONS_RESULT);
    });

    test('should remain same as there is condition is present', () => {
        const result = removeEmptyArrays(PROPERTY_CONDITIONS);
        expect(result).toEqual(PROPERTY_CONDITIONS);
    });

    test('should remain same as conditions are complex but not empty', () => {
        const result = removeEmptyArrays(COMPLEX_CONDITIONS);
        expect(result).toEqual(COMPLEX_CONDITIONS);
    });

    test('preserves null values in config settings', () => {
        const result = removeEmptyArrays(NULL_CONDITIONS);
        expect(result).toEqual(NULL_CONDITIONS);
        expect(result.source.config.profiling.profile_table_size_limit).toBeNull();
    });
});
