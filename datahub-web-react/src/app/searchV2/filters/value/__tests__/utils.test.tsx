import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    EntityFilterField,
    FieldType,
    FilterField,
    FilterOperatorType,
    FilterValueOption,
} from '@app/searchV2/filters/types';
import {
    deduplicateOptions,
    getDefaultFieldOperatorType,
    getEntityTypeFilterValueDisplayName,
    mapFilterCountsToZero,
    useFilterOptionsBySearchQuery,
    useLoadAggregationOptions,
    useLoadSearchOptions,
} from '@app/searchV2/filters/value/utils';
import { FILTER_DELIMITER } from '@app/searchV2/utils/constants';
import useGetSearchQueryInputs from '@src/app/search/useGetSearchQueryInputs';

import {
    useAggregateAcrossEntitiesQuery,
    useGetAutoCompleteMultipleResultsQuery,
    useGetSearchResultsForMultipleQuery,
} from '@graphql/search.generated';
import { Dataset, EntityType } from '@types';

// Mock the GraphQL queries
vi.mock('@graphql/search.generated', async (importOriginal) => {
    const actual = (await importOriginal()) as any;
    return {
        ...actual,
        useAggregateAcrossEntitiesQuery: vi.fn(),
        useGetAutoCompleteMultipleResultsQuery: vi.fn(),
        useGetSearchResultsForMultipleQuery: vi.fn(),
    };
});

// Mock the search query inputs hook
vi.mock('@src/app/search/useGetSearchQueryInputs', () => ({
    default: vi.fn(),
}));

// Mock the entity registry hook
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: vi.fn(() => ({
        getDisplayName: vi.fn((type: EntityType, entity: any) => entity?.name || 'Test Entity'),
        getCollectionName: vi.fn((type: string) => `${type.toLowerCase()}s`),
    })),
}));

describe('deduplicateOptions', () => {
    it('should remove options from moreOptions that have the same value as baseOptions', () => {
        const baseOptions: FilterValueOption[] = [
            { value: 'option1', displayName: 'Option 1' },
            { value: 'option2', displayName: 'Option 2' },
        ];

        const moreOptions: FilterValueOption[] = [
            { value: 'option2', displayName: 'Option 2 Duplicate' },
            { value: 'option3', displayName: 'Option 3' },
            { value: 'option4', displayName: 'Option 4' },
        ];

        const result = deduplicateOptions(baseOptions, moreOptions);

        expect(result).toHaveLength(2);
        expect(result).toEqual([
            { value: 'option3', displayName: 'Option 3' },
            { value: 'option4', displayName: 'Option 4' },
        ]);
    });

    it('should return all moreOptions when there are no duplicates', () => {
        const baseOptions: FilterValueOption[] = [
            { value: 'option1', displayName: 'Option 1' },
            { value: 'option2', displayName: 'Option 2' },
        ];

        const moreOptions: FilterValueOption[] = [
            { value: 'option3', displayName: 'Option 3' },
            { value: 'option4', displayName: 'Option 4' },
        ];

        const result = deduplicateOptions(baseOptions, moreOptions);

        expect(result).toHaveLength(2);
        expect(result).toEqual(moreOptions);
    });

    it('should return empty array when all moreOptions are duplicates', () => {
        const baseOptions: FilterValueOption[] = [
            { value: 'option1', displayName: 'Option 1' },
            { value: 'option2', displayName: 'Option 2' },
        ];

        const moreOptions: FilterValueOption[] = [
            { value: 'option1', displayName: 'Option 1 Duplicate' },
            { value: 'option2', displayName: 'Option 2 Duplicate' },
        ];

        const result = deduplicateOptions(baseOptions, moreOptions);

        expect(result).toHaveLength(0);
        expect(result).toEqual([]);
    });

    it('should handle empty arrays correctly', () => {
        expect(deduplicateOptions([], [])).toEqual([]);

        const moreOptions: FilterValueOption[] = [{ value: 'option1', displayName: 'Option 1' }];
        expect(deduplicateOptions([], moreOptions)).toEqual(moreOptions);

        const baseOptions: FilterValueOption[] = [{ value: 'option1', displayName: 'Option 1' }];
        expect(deduplicateOptions(baseOptions, [])).toEqual([]);
    });
});

describe('mapFilterCountsToZero', () => {
    it('should set all option counts to 0', () => {
        const options: FilterValueOption[] = [
            { value: 'option1', count: 5, displayName: 'Option 1' },
            { value: 'option2', count: 10, displayName: 'Option 2' },
            { value: 'option3', count: undefined, displayName: 'Option 3' },
        ];

        const result = mapFilterCountsToZero(options);

        expect(result).toEqual([
            { value: 'option1', count: 0, displayName: 'Option 1' },
            { value: 'option2', count: 0, displayName: 'Option 2' },
            { value: 'option3', count: 0, displayName: 'Option 3' },
        ]);
    });

    it('should handle empty array', () => {
        const result = mapFilterCountsToZero([]);
        expect(result).toEqual([]);
    });
});

describe('useLoadAggregationOptions', () => {
    const mockField: FilterField = {
        field: 'platform',
        displayName: 'Platform',
        type: FieldType.TEXT,
    };

    const mockSearchQueryInputs = {
        entityFilters: [EntityType.Dataset],
        query: 'test query',
        orFilters: [],
        viewUrn: 'urn:li:view:test',
    };

    beforeEach(() => {
        vi.clearAllMocks();
        (useGetSearchQueryInputs as any).mockReturnValue(mockSearchQueryInputs);
    });

    it('should return loading false and empty options when not visible', () => {
        (useAggregateAcrossEntitiesQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        const { result } = renderHook(() =>
            useLoadAggregationOptions({
                field: mockField,
                visible: false,
                includeCounts: true,
            }),
        );

        expect(result.current).toEqual({
            loading: false,
            options: [],
        });
    });

    it('should return loading true while query is loading', () => {
        (useAggregateAcrossEntitiesQuery as any).mockReturnValue({
            data: null,
            loading: true,
        });

        const { result } = renderHook(() =>
            useLoadAggregationOptions({
                field: mockField,
                visible: true,
                includeCounts: true,
            }),
        );

        expect(result.current.loading).toBe(true);
        expect(result.current.options).toEqual([]);
    });

    it('should process aggregation data correctly with counts', () => {
        const mockAggregationData = {
            aggregateAcrossEntities: {
                facets: [
                    {
                        field: 'platform',
                        aggregations: [
                            { value: 'snowflake', count: 10, entity: null },
                            { value: 'bigquery', count: 5, entity: null },
                        ],
                    },
                ],
            },
        };

        (useAggregateAcrossEntitiesQuery as any).mockReturnValue({
            data: mockAggregationData,
            loading: false,
        });

        const { result } = renderHook(() =>
            useLoadAggregationOptions({
                field: mockField,
                visible: true,
                includeCounts: true,
            }),
        );

        expect(result.current.loading).toBe(false);
        expect(result.current.options).toHaveLength(2);
        expect(result.current.options[0]).toEqual({
            value: 'snowflake',
            entity: null,
            count: 10,
            displayName: undefined,
        });
        expect(result.current.options[1]).toEqual({
            value: 'bigquery',
            entity: null,
            count: 5,
            displayName: undefined,
        });
    });

    it('should process aggregation data correctly without counts', () => {
        const mockAggregationData = {
            aggregateAcrossEntities: {
                facets: [
                    {
                        field: 'platform',
                        aggregations: [
                            { value: 'snowflake', count: 10, entity: null },
                            { value: 'bigquery', count: 5, entity: null },
                        ],
                    },
                ],
            },
        };

        (useAggregateAcrossEntitiesQuery as any).mockReturnValue({
            data: mockAggregationData,
            loading: false,
        });

        const { result } = renderHook(() =>
            useLoadAggregationOptions({
                field: mockField,
                visible: true,
                includeCounts: false,
            }),
        );

        expect(result.current.loading).toBe(false);
        expect(result.current.options).toHaveLength(2);
        expect(result.current.options[0].count).toBeUndefined();
        expect(result.current.options[1].count).toBeUndefined();
    });

    it('should filter out options with no count when removeOptionsWithNoCount is true', () => {
        const mockAggregationData = {
            aggregateAcrossEntities: {
                facets: [
                    {
                        field: 'platform',
                        aggregations: [
                            { value: 'snowflake', count: 10, entity: null },
                            { value: 'bigquery', count: 0, entity: null },
                            { value: 'redshift', count: 5, entity: null },
                        ],
                    },
                ],
            },
        };

        (useAggregateAcrossEntitiesQuery as any).mockReturnValue({
            data: mockAggregationData,
            loading: false,
        });

        const { result } = renderHook(() =>
            useLoadAggregationOptions({
                field: mockField,
                visible: true,
                includeCounts: true,
                removeOptionsWithNoCount: true,
            }),
        );

        expect(result.current.loading).toBe(false);
        expect(result.current.options).toHaveLength(2);
        expect(result.current.options.map((opt) => opt.value)).toEqual(['snowflake', 'redshift']);
    });

    it('should handle missing facet data', () => {
        const mockAggregationData = {
            aggregateAcrossEntities: {
                facets: [
                    {
                        field: 'other_field',
                        aggregations: [{ value: 'test', count: 1, entity: null }],
                    },
                ],
            },
        };

        (useAggregateAcrossEntitiesQuery as any).mockReturnValue({
            data: mockAggregationData,
            loading: false,
        });

        const { result } = renderHook(() =>
            useLoadAggregationOptions({
                field: mockField,
                visible: true,
                includeCounts: true,
            }),
        );

        expect(result.current.loading).toBe(false);
        expect(result.current.options).toEqual([]);
    });

    it('should handle null aggregations', () => {
        const mockAggregationData = {
            aggregateAcrossEntities: {
                facets: [
                    {
                        field: 'platform',
                        aggregations: null,
                    },
                ],
            },
        };

        (useAggregateAcrossEntitiesQuery as any).mockReturnValue({
            data: mockAggregationData,
            loading: false,
        });

        const { result } = renderHook(() =>
            useLoadAggregationOptions({
                field: mockField,
                visible: true,
                includeCounts: true,
            }),
        );

        expect(result.current.loading).toBe(false);
        expect(result.current.options).toEqual([]);
    });
});

describe('getEntityTypeFilterValueDisplayName', () => {
    const mockEntityRegistry = {
        getCollectionName: vi.fn((type: string) => `${type.toLowerCase()}s`),
    };

    it('should handle values with filter delimiter', () => {
        const result = getEntityTypeFilterValueDisplayName(
            `DATASET${FILTER_DELIMITER}subType`,
            mockEntityRegistry as any,
        );
        expect(result).toBe('SubType');
    });

    it('should handle values without filter delimiter', () => {
        const result = getEntityTypeFilterValueDisplayName('DATASET', mockEntityRegistry as any);
        expect(result).toBe('datasets');
        expect(mockEntityRegistry.getCollectionName).toHaveBeenCalledWith('DATASET');
    });
});

describe('getDefaultFieldOperatorType', () => {
    it('should return CONTAINS for TEXT field type', () => {
        const field: FilterField = {
            field: 'test',
            displayName: 'Test Field',
            type: FieldType.TEXT,
        };

        const result = getDefaultFieldOperatorType(field);
        expect(result).toBe(FilterOperatorType.CONTAINS);
    });

    it('should return EQUALS for non-TEXT field types', () => {
        const enumField: FilterField = {
            field: 'test',
            displayName: 'Test Enum Field',
            type: FieldType.ENUM,
        };

        const booleanField: FilterField = {
            field: 'test',
            displayName: 'Test Boolean Field',
            type: FieldType.BOOLEAN,
        };

        expect(getDefaultFieldOperatorType(enumField)).toBe(FilterOperatorType.EQUALS);
        expect(getDefaultFieldOperatorType(booleanField)).toBe(FilterOperatorType.EQUALS);
    });
});

describe('useFilterOptionsBySearchQuery', () => {
    const mockOptions: FilterValueOption[] = [
        { value: 'option1', displayName: 'First Option' },
        { value: 'option2', displayName: 'Second Option' },
        { value: 'option3', displayName: 'Third Option' },
        { value: 'option4', displayName: 'Apple' },
    ];

    it('should return all options when no search query', () => {
        const { result } = renderHook(() => useFilterOptionsBySearchQuery(mockOptions, undefined));

        expect(result.current).toEqual(mockOptions);
    });

    it('should return all options when search query is empty', () => {
        const { result } = renderHook(() => useFilterOptionsBySearchQuery(mockOptions, ''));

        expect(result.current).toEqual(mockOptions);
    });

    it('should filter options by display name', () => {
        const { result } = renderHook(() => useFilterOptionsBySearchQuery(mockOptions, 'first'));

        expect(result.current).toHaveLength(1);
        expect(result.current[0].value).toBe('option1');
    });

    it('should filter options case-insensitively', () => {
        const { result } = renderHook(() => useFilterOptionsBySearchQuery(mockOptions, 'FIRST'));

        expect(result.current).toHaveLength(1);
        expect(result.current[0].value).toBe('option1');
    });

    it('should filter options by value when no display name', () => {
        const optionsWithoutDisplayName: FilterValueOption[] = [
            { value: 'option1', displayName: undefined },
            { value: 'option2', displayName: undefined },
        ];

        const { result } = renderHook(() => useFilterOptionsBySearchQuery(optionsWithoutDisplayName, 'option1'));

        expect(result.current).toHaveLength(1);
        expect(result.current[0].value).toBe('option1');
    });

    it('should return empty array when no matches', () => {
        const { result } = renderHook(() => useFilterOptionsBySearchQuery(mockOptions, 'nonexistent'));

        expect(result.current).toHaveLength(0);
    });

    it('should handle options with entity', () => {
        const optionsWithEntity: FilterValueOption[] = [
            {
                value: 'urn:li:dataset:1',
                entity: { type: EntityType.Dataset, name: 'My Dataset' } as Dataset,
            },
            {
                value: 'urn:li:dataset:2',
                entity: { type: EntityType.Dataset, name: 'Another Dataset' } as Dataset,
            },
        ];

        const { result } = renderHook(() => useFilterOptionsBySearchQuery(optionsWithEntity, 'my'));

        expect(result.current).toHaveLength(1);
        expect(result.current[0].value).toBe('urn:li:dataset:1');
    });

    it('should handle special characters in search query', () => {
        const optionsWithSpecialChars: FilterValueOption[] = [
            { value: 'option1', displayName: 'Test_Option_1' },
            { value: 'option2', displayName: 'Different Option' },
        ];

        const { result } = renderHook(() => useFilterOptionsBySearchQuery(optionsWithSpecialChars, 'test_option'));

        // The filter normalizes by removing underscores/spaces, so both "Test_Option_1" and "test_option" match
        expect(result.current).toHaveLength(1);
        expect(result.current[0].value).toBe('option1');
    });
});

describe('useLoadSearchOptions', () => {
    const mockField: EntityFilterField = {
        field: 'tags',
        type: FieldType.ENTITY,
        displayName: 'Tags',
        entityTypes: [EntityType.Tag],
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should skip queries when skip is true', () => {
        (useGetAutoCompleteMultipleResultsQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });
        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        const { result } = renderHook(() => useLoadSearchOptions(mockField, 'test', true));

        // When skip is true, both queries should be skipped internally
        expect(result.current.loading).toBe(false);
        expect(result.current.options).toEqual([]);
    });

    it('should skip queries when query is not provided', () => {
        (useGetAutoCompleteMultipleResultsQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });
        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        const { result } = renderHook(() => useLoadSearchOptions(mockField, undefined, false));

        // When query is undefined, should skip and return empty
        expect(result.current.loading).toBe(false);
        expect(result.current.options).toEqual([]);
    });

    it('should return autocomplete results when query is provided', () => {
        const mockAutoCompleteData = {
            autoCompleteForMultiple: {
                suggestions: [
                    {
                        type: EntityType.Tag,
                        entities: [
                            { urn: 'urn:li:tag:1', name: 'Tag 1' },
                            { urn: 'urn:li:tag:2', name: 'Tag 2' },
                        ],
                    },
                ],
            },
        };

        (useGetAutoCompleteMultipleResultsQuery as any).mockReturnValue({
            data: mockAutoCompleteData,
            loading: false,
        });

        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        const { result } = renderHook(() => useLoadSearchOptions(mockField, 'test', false));

        expect(result.current.loading).toBe(false);
        expect(result.current.options).toHaveLength(2);
        expect(result.current.options[0].value).toBe('urn:li:tag:1');
        expect(result.current.options[1].value).toBe('urn:li:tag:2');
    });

    it('should return search results when no query but not skipped', () => {
        const mockSearchData = {
            searchAcrossEntities: {
                searchResults: [
                    { entity: { urn: 'urn:li:dataset:1', name: 'Dataset 1' } },
                    { entity: { urn: 'urn:li:dataset:2', name: 'Dataset 2' } },
                ],
            },
        };

        (useGetAutoCompleteMultipleResultsQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({
            data: mockSearchData,
            loading: false,
        });

        // When query is empty string, autocomplete is skipped but search runs
        const { result } = renderHook(() => useLoadSearchOptions(mockField, '', false));

        expect(result.current.loading).toBe(false);
        expect(result.current.options).toHaveLength(2);
        expect(result.current.options[0].value).toBe('urn:li:dataset:1');
    });

    it('should return empty options when no data available', () => {
        (useGetAutoCompleteMultipleResultsQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        const { result } = renderHook(() => useLoadSearchOptions(mockField, 'test', false));

        expect(result.current.loading).toBe(false);
        expect(result.current.options).toEqual([]);
    });

    it('should return loading state correctly', () => {
        (useGetAutoCompleteMultipleResultsQuery as any).mockReturnValue({
            data: null,
            loading: true,
        });

        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        const { result } = renderHook(() => useLoadSearchOptions(mockField, 'test', false));

        expect(result.current.loading).toBe(true);
    });

    it('should combine loading states from both queries', () => {
        (useGetAutoCompleteMultipleResultsQuery as any).mockReturnValue({
            data: null,
            loading: false,
        });

        (useGetSearchResultsForMultipleQuery as any).mockReturnValue({
            data: null,
            loading: true,
        });

        const { result } = renderHook(() => useLoadSearchOptions(mockField, '', false));

        expect(result.current.loading).toBe(true);
    });
});
