import { FolderFilled } from '@ant-design/icons';
import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import { FieldType, FilterField } from '@app/searchV2/filters/types';
import {
    PlatformIcon,
    canCreateViewFromFilters,
    combineAggregations,
    deduplicateAggregations,
    filterEmptyAggregations,
    filterOptionsWithSearch,
    getFilterDisplayName,
    getFilterEntity,
    getFilterIconAndLabel,
    getFilterOptions,
    getNewFilters,
    getNumActiveFiltersForFilter,
    getNumActiveFiltersForGroupOfFilters,
    getStructuredPropFilterDisplayName,
    isAnyOptionSelected,
    isFilterOptionSelected,
} from '@app/searchV2/filters/utils';
import { ENTITY_SUB_TYPE_FILTER_NAME } from '@app/searchV2/utils/constants';
import { dataPlatform, dataPlatformInstance, dataset1, glossaryTerm1, user1 } from '@src/Mocks';
import { DATE_TYPE_URN } from '@src/app/shared/constants';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { AggregationMetadata, EntityType } from '@types';

describe('filter utils - getNewFilters', () => {
    it('should get the correct list of filters when adding filters where the filter field did not already exist', () => {
        const activeFilters = [{ field: 'entity', values: ['test'] }];
        const selectedFilterValues = ['one', 'two'];
        const newFilters = getNewFilters('platform', activeFilters, selectedFilterValues);
        expect(newFilters).toMatchObject([
            { field: 'entity', values: ['test'] },
            { field: 'platform', values: ['one', 'two'] },
        ]);
    });

    it('should get the correct list of filters when adding filters where the filter field does already exist', () => {
        const activeFilters = [{ field: 'entity', values: ['test'] }];
        const selectedFilterValues = ['one', 'two'];
        const newFilters = getNewFilters('entity', activeFilters, selectedFilterValues);
        expect(newFilters).toMatchObject([{ field: 'entity', values: ['one', 'two'] }]);
    });

    it('should get the correct list of filters when adding filters where the filter field does already exist with other filters', () => {
        const activeFilters = [
            { field: 'entity', values: ['test'] },
            { field: 'platform', values: ['one'] },
        ];
        const selectedFilterValues = ['one', 'two'];
        const newFilters = getNewFilters('platform', activeFilters, selectedFilterValues);
        expect(newFilters).toMatchObject([
            { field: 'entity', values: ['test'] },
            { field: 'platform', values: ['one', 'two'] },
        ]);
    });

    it('should get the correct list of filters when removing filters all of the filters for a filter type', () => {
        const activeFilters = [
            { field: 'entity', values: ['test'] },
            { field: 'platform', values: ['one'] },
        ];
        const selectedFilterValues = [];
        const newFilters = getNewFilters('platform', activeFilters, selectedFilterValues);
        expect(newFilters).toMatchObject([{ field: 'entity', values: ['test'] }]);
    });

    it('should get the correct list of filters when removing filters one of multiple of the filters for a filter type', () => {
        const activeFilters = [
            { field: 'entity', values: ['test'] },
            { field: 'platform', values: ['one', 'two'] },
        ];
        const selectedFilterValues = ['two'];
        const newFilters = getNewFilters('platform', activeFilters, selectedFilterValues);
        expect(newFilters).toMatchObject([
            { field: 'entity', values: ['test'] },
            { field: 'platform', values: ['two'] },
        ]);
    });
});

describe('filter utils - isFilterOptionSelected', () => {
    const selectedFilterOptions = [
        { value: 'one', field: 'test' },
        { value: 'two', field: 'test' },
        { value: 'DATASETS', field: 'test' },
    ];
    it('should return true if the given filter value exists in the list', () => {
        expect(isFilterOptionSelected(selectedFilterOptions, 'two')).toBe(true);
    });

    it('should return false if the given filter value does not exist in the list', () => {
        expect(isFilterOptionSelected(selectedFilterOptions, 'testing123')).toBe(false);
    });

    it('should return false if the given filter value does not exist in the list, even if values are similar', () => {
        expect(isFilterOptionSelected(selectedFilterOptions, 'tw')).toBe(false);
    });

    it('should return true if a parent filter is selected', () => {
        expect(isFilterOptionSelected(selectedFilterOptions, 'DATASETS␞view')).toBe(true);
    });
});

describe('filter utils - isAnyOptionSelected', () => {
    const selectedFilterOptions = [
        { value: 'one', field: 'test' },
        { value: 'two', field: 'test' },
        { value: 'DATASETS', field: 'test' },
    ];
    it('should return true if any of the given filter values exists in the selected values list', () => {
        expect(isAnyOptionSelected(selectedFilterOptions, ['two', 'four'])).toBe(true);
    });

    it('should return false if none of the given filter values exists in the selected values list', () => {
        expect(isAnyOptionSelected(selectedFilterOptions, ['three', 'four'])).toBe(false);
    });
});

describe('filter utils - getFilterIconAndLabel', () => {
    const mockEntityRegistry = getTestEntityRegistry();

    it('should get the correct icon and label for entity filters', () => {
        const { icon, label } = getFilterIconAndLabel('entity', EntityType.Dataset, mockEntityRegistry, dataset1);

        expect(icon).toMatchObject(mockEntityRegistry.getIcon(EntityType.Dataset, 12, IconStyleType.ACCENT));
        expect(label).toBe(mockEntityRegistry.getCollectionName(EntityType.Dataset));
    });

    it('should get the correct icon and label for platform filters', () => {
        const { icon, label } = getFilterIconAndLabel('platform', dataPlatform.urn, mockEntityRegistry, dataPlatform);

        expect(icon).toMatchObject(<PlatformIcon src={dataPlatform.properties.logoUrl} />);
        expect(label).toBe(mockEntityRegistry.getDisplayName(EntityType.DataPlatform, dataPlatform));
    });

    it('should get the correct icon and label for filters with associated entity', () => {
        const { icon, label } = getFilterIconAndLabel('domains', glossaryTerm1.urn, mockEntityRegistry, glossaryTerm1);

        expect(icon).toMatchObject(mockEntityRegistry.getIcon(EntityType.GlossaryTerm, 12, IconStyleType.ACCENT));
        expect(label).toBe(mockEntityRegistry.getDisplayName(EntityType.GlossaryTerm, glossaryTerm1));
    });

    it('should get the correct icon and label for filters with associated data platform instance entity', () => {
        const { icon, label } = getFilterIconAndLabel(
            'domains',
            glossaryTerm1.urn,
            mockEntityRegistry,
            dataPlatformInstance,
        );

        expect(icon).toMatchObject(<PlatformIcon src={dataPlatformInstance.platform.properties.logoUrl} />);
        expect(label).toBe(dataPlatformInstance.instanceId);
    });

    it('should get the correct icon and label for filters with no associated entity', () => {
        const { icon, label } = getFilterIconAndLabel('origin', 'PROD', mockEntityRegistry, null);

        expect(icon).toBe(null);
        expect(label).toBe('PROD');
    });

    it('should get the correct icon and label for browse v2 filters', () => {
        const { icon, label } = getFilterIconAndLabel(
            'browsePathV2',
            '␟long-tail-companions␟view',
            mockEntityRegistry,
            null,
        );

        expect(icon).toMatchObject(<FolderFilled style={{ fontSize: undefined }} />);
        expect(label).toBe('view');
    });

    it('should override the filter label if we provide an override', () => {
        const { icon, label } = getFilterIconAndLabel(
            'browsePathV2',
            '␟long-tail-companions␟view',
            mockEntityRegistry,
            null,
            12,
            'TESTING',
        );

        expect(icon).toMatchObject(<FolderFilled style={{ fontSize: 12 }} />);
        expect(label).toBe('TESTING');
    });
});

describe('filter utils - getFilterEntity', () => {
    const availableFilters = [
        {
            field: 'owners',
            aggregations: [{ value: 'chris', count: 15 }],
        },
        {
            field: 'platform',
            aggregations: [
                { value: 'snowflake', count: 12 },
                { value: 'dbt', count: 4, entity: dataPlatform },
            ],
        },
    ];

    it('should find and return the filter entity given a filter field and value and availableFilters', () => {
        expect(getFilterEntity('platform', 'dbt', availableFilters)).toMatchObject(dataPlatform);
    });

    it('should return null if the given filter has no associated entity in availableFilters', () => {
        expect(getFilterEntity('platform', 'nonExistent', availableFilters)).toBe(null);
    });
});

describe('filter utils - getNumActiveFiltersForFilter', () => {
    const activeFilters = [
        { field: 'owners', values: ['chris', 'john'] },
        { field: 'platform', values: ['dbt'] },
    ];

    it('should get the number of active filters for a given filter group when there are active filters', () => {
        const filter = {
            field: 'owners',
            aggregations: [
                { value: 'chris', count: 15 },
                { value: 'john', count: 11 },
            ],
        };

        expect(getNumActiveFiltersForFilter(activeFilters, filter)).toBe(2);
    });

    it('should get the number of active filters for a given filter group when there are no active filters', () => {
        const filter = {
            field: 'tags',
            aggregations: [{ value: 'tag', count: 15 }],
        };

        expect(getNumActiveFiltersForFilter(activeFilters, filter)).toBe(0);
    });
});

describe('filter utils - getNumActiveFiltersForGroupOfFilters', () => {
    const activeFilters = [
        { field: 'owners', values: ['chris', 'john'] },
        { field: 'platform', values: ['dbt'] },
    ];

    it('should get the number of active filters for a given filter group when there are active filters', () => {
        const filters = [
            {
                field: 'owners',
                aggregations: [
                    { value: 'chris', count: 15 },
                    { value: 'john', count: 11 },
                ],
            },
            {
                field: 'tags',
                aggregations: [{ value: 'tag1', count: 15 }],
            },
        ];

        expect(getNumActiveFiltersForGroupOfFilters(activeFilters, filters)).toBe(2);
    });

    it('should get the number of active filters for a given filter group when there are no active filters', () => {
        const filters = [
            {
                field: 'tags',
                aggregations: [{ value: 'tag', count: 15 }],
            },
            {
                field: 'origin',
                aggregations: [
                    { value: 'prod', count: 15 },
                    { value: 'dev', count: 15 },
                ],
            },
        ];

        expect(getNumActiveFiltersForGroupOfFilters(activeFilters, filters)).toBe(0);
    });
});

describe('filter utils - combineAggregations', () => {
    const originalAggs = [
        { value: 'aditya', count: 10 },
        { value: 'maggie', count: 5 },
        { value: 'brittanie', count: 500 },
    ];

    it('should combine aggregations given some original aggregations, a list of new facets, and a filter field', () => {
        const newFacets = [
            {
                field: 'platform',
                aggregations: [{ value: 'dbt', count: 15 }],
            },
            {
                field: 'owners',
                aggregations: [
                    { value: 'chris', count: 15 },
                    { value: 'john', count: 20 },
                ],
            },
        ];

        const combinedAggregations = combineAggregations('owners', originalAggs, newFacets);

        expect(combinedAggregations).toMatchObject([
            { value: 'aditya', count: 10 },
            { value: 'maggie', count: 5 },
            { value: 'brittanie', count: 500 },
            { value: 'chris', count: 15 },
            { value: 'john', count: 20 },
        ]);
    });

    it('should return the original aggs when newFacets are not provided', () => {
        const combinedAggregations = combineAggregations('owners', originalAggs);

        expect(combinedAggregations).toMatchObject([
            { value: 'aditya', count: 10 },
            { value: 'maggie', count: 5 },
            { value: 'brittanie', count: 500 },
        ]);
    });
});

describe('filter utils - filterEmptyAggregations', () => {
    const originalAggs = [
        { value: 'aditya', count: 10 },
        { value: 'maggie', count: 5 },
        { value: 'brittanie', count: 0 },
        { value: 'john', count: 0 },
    ];

    it('should filter out empty aggregations unless they are in activeFilters', () => {
        const activeFilters = [
            { field: 'owners', values: ['chris', 'john'] },
            { field: 'platform', values: ['dbt'] },
        ];

        const filteredAggregations = filterEmptyAggregations(originalAggs, activeFilters);

        expect(filteredAggregations).toMatchObject([
            { value: 'aditya', count: 10 },
            { value: 'maggie', count: 5 },
            { value: 'john', count: 0 },
        ]);
    });
});

describe('deduplicateAggregations() deduplicateAggregations method', () => {
    // Happy Path Tests
    describe('Happy Paths', () => {
        it('should return an empty array when both baseAggs and secondaryAggs are empty', () => {
            const baseAggs: AggregationMetadata[] = [];
            const secondaryAggs: AggregationMetadata[] = [];
            const result = deduplicateAggregations(baseAggs, secondaryAggs);
            expect(result).toEqual([]);
        });

        it('should return secondaryAggs when baseAggs is empty', () => {
            const baseAggs: AggregationMetadata[] = [];
            const secondaryAggs: AggregationMetadata[] = [
                { count: 0, value: 'value1' },
                { count: 0, value: 'value2' },
            ];
            const result = deduplicateAggregations(baseAggs, secondaryAggs);
            expect(result).toEqual(secondaryAggs);
        });

        it('should return an empty array when all secondaryAggs are in baseAggs', () => {
            const baseAggs: AggregationMetadata[] = [
                { count: 0, value: 'value1' },
                { count: 0, value: 'value2' },
            ];
            const secondaryAggs: AggregationMetadata[] = [
                { count: 1, value: 'value1' },
                { count: 2, value: 'value2' },
            ];
            const result = deduplicateAggregations(baseAggs, secondaryAggs);
            expect(result).toEqual([]);
        });

        it('should return only the unique secondaryAggs not present in baseAggs', () => {
            const baseAggs: AggregationMetadata[] = [{ count: 0, value: 'value1' }];
            const secondaryAggs: AggregationMetadata[] = [
                { count: 0, value: 'value1' },
                { count: 2, value: 'value2' },
            ];
            const result = deduplicateAggregations(baseAggs, secondaryAggs);
            expect(result).toEqual([{ count: 2, value: 'value2' }]);
        });
    });

    // Edge Case Tests
    describe('Edge Cases', () => {
        it('should handle case sensitivity correctly', () => {
            const baseAggs: AggregationMetadata[] = [{ count: 0, value: 'Value1' }];
            const secondaryAggs: AggregationMetadata[] = [
                { count: 0, value: 'value1' },
                { count: 0, value: 'Value2' },
            ];
            const result = deduplicateAggregations(baseAggs, secondaryAggs);
            expect(result).toEqual([
                { count: 0, value: 'value1' },
                { count: 0, value: 'Value2' },
            ]);
        });

        it('should handle large arrays efficiently', () => {
            const baseAggs: AggregationMetadata[] = Array.from({ length: 1000 }, (_, i) => ({
                count: 0,
                value: `value${i}`,
            }));
            const secondaryAggs: AggregationMetadata[] = Array.from({ length: 2000 }, (_, i) => ({
                count: 0,
                value: `value${i}`,
            }));
            const result = deduplicateAggregations(baseAggs, secondaryAggs);
            expect(result.length).toBe(1000);
            expect(result[0]).toEqual({ count: 0, value: 'value1000' });
        });
    });
});

describe('filter utils - getFilterOptions', () => {
    const originalAggs = [
        { value: 'aditya', count: 10 },
        { value: 'maggie', count: 5 },
        { value: 'brittanie', count: 0 },
        { value: 'john', count: 0 },
    ];
    const selectedFilterOptions = [
        { value: 'aditya', field: 'owners' },
        { value: 'chris', field: 'owners' },
    ];

    it('should convert aggregations into filterOptions while adding missing filterOptions to the beginning', () => {
        const filterOptions = getFilterOptions('glossaryTerms', originalAggs, selectedFilterOptions);

        expect(filterOptions).toMatchObject([
            { value: 'chris' },
            { value: 'aditya', count: 10 },
            { value: 'maggie', count: 5 },
            { value: 'brittanie', count: 0 },
            { value: 'john', count: 0 },
        ]);
    });

    it('should add auto complete results to the returned list', () => {
        const autoCompleteResults = {
            autoCompleteForMultiple: { suggestions: [{ type: EntityType.CorpUser, entities: [user1] }] },
        };
        const filterOptions = getFilterOptions(
            'owners',
            originalAggs,
            selectedFilterOptions,
            autoCompleteResults as any,
        );

        expect(filterOptions).toMatchObject([
            { value: 'chris', field: 'owners' },
            { value: 'aditya', count: 10, field: 'owners' },
            { value: 'maggie', count: 5, field: 'owners' },
            { value: 'brittanie', count: 0, field: 'owners' },
            { value: 'john', count: 0, field: 'owners' },
            { value: user1.urn, entity: user1, field: 'owners' },
        ]);
    });
});

describe('filter utils - filterOptionsWithSearch', () => {
    it('should return true if the name includes the search query', () => {
        expect(filterOptionsWithSearch('test', 'testing123')).toBe(true);
    });

    it('should return false if the name includes the search query', () => {
        expect(filterOptionsWithSearch('test', 'hello')).toBe(false);
    });

    it('should return true if there is no search query', () => {
        expect(filterOptionsWithSearch('', 'hello')).toBe(true);
    });

    it('should ignore underscores and spaces', () => {
        expect(filterOptionsWithSearch('dataproduct', 'DATA_PRODUCT')).toBe(true);
        expect(filterOptionsWithSearch('data product', 'DATA_PRODUCT')).toBe(true);
    });
});

describe('filter utils - canCreateViewFromFilters', () => {
    it('should return false if there is mixing of entity type and subtypes in the nested subtypes filter', () => {
        const activeFilters = [
            { field: 'platform', values: ['one', 'two'] },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASETS', 'CONTAINERS␞schema'] },
        ];
        expect(canCreateViewFromFilters(activeFilters)).toBe(false);
    });

    it('should return true if there is no mixing of entity type and subtypes in the nested subtypes filter', () => {
        const activeFilters = [
            { field: 'platform', values: ['one', 'two'] },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASETS', 'CONTAINERS'] },
        ];
        expect(canCreateViewFromFilters(activeFilters)).toBe(true);
    });
});

describe('filter utils - getStructuredPropFilterDisplayName', () => {
    it('should return undefined if the field is not a structured property filter', () => {
        expect(getStructuredPropFilterDisplayName('test', 'test')).toBe(undefined);
    });

    it('should return undefined if the value is an entity since those are handled separately', () => {
        expect(getStructuredPropFilterDisplayName('structuredProperties.steward', 'urn:li:corpuser:admin')).toBe(
            undefined,
        );
    });

    it('should return a formatted date if the structured property has type date', () => {
        const structuredProperty = { definition: { valueType: { urn: DATE_TYPE_URN } } } as any;
        expect(
            getStructuredPropFilterDisplayName(
                'structuredProperties.deprecationDate',
                '1727740800000',
                structuredProperty,
            ),
        ).toBe('10/01/2024');
    });

    it('should return a properly formatted number if it is a number type', () => {
        expect(getStructuredPropFilterDisplayName('structuredProperties.retentionTime', '90.0')).toBe('90');
    });

    it('should strip rich text formatting to be displayed', () => {
        expect(
            getStructuredPropFilterDisplayName(
                'structuredProperties.retentionTime',
                '`test` _value_ for a [rich](www.google.com) text **situation** right here!',
            ),
        ).toBe('test value for a rich text situation right here!');
    });
});

describe('filter utils - getFilterDisplayName', () => {
    it('should return the displayName for an option if it exists', () => {
        const option = { value: 'testValue', displayName: 'test name' };
        const field: FilterField = { type: FieldType.ENUM, field: 'test', displayName: 'test' };
        expect(getFilterDisplayName(option, field)).toBe('test name');
    });

    it('should return undefined if no display name and field is not a structured property filter', () => {
        const option = { value: 'testValue' };
        const field: FilterField = { type: FieldType.ENUM, field: 'structuredProperties.test', displayName: 'test' };
        expect(getFilterDisplayName(option, field)).toBe('testValue');
    });

    it('should return the structured property value properly if this is a structured property filter (structured prop value is tested above)', () => {
        const option = { value: 'testValue' };
        const field: FilterField = { type: FieldType.ENUM, field: 'test', displayName: 'test' };
        expect(getFilterDisplayName(option, field)).toBe(undefined);
    });
});
