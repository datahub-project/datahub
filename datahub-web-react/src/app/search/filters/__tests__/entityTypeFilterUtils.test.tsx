import React from 'react';
import { getTestEntityRegistry } from '../../../../utils/test-utils/TestPageContainer';
import {
    LEGACY_ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    ENTITY_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '../../utils/constants';
import {
    getDisplayedFilterOptions,
    getInitialSelectedOptions,
    getInitialSelectedOptionsFromAggregations,
    getNumActiveFilters,
} from '../EntityTypeFilter/entityTypeFilterUtils';
import FilterOption from '../FilterOption';

const mockData = {
    aggregateAcrossEntities: {
        facets: [
            {
                field: ENTITY_SUB_TYPE_FILTER_NAME,
                aggregations: [
                    { value: 'DATASET', count: 12 },
                    { value: 'DATASET␞table', count: 6 },
                    { value: 'DATASET␞view', count: 6 },
                    { value: 'CONTAINER', count: 6 },
                    { value: 'CONTAINER␞test', count: 3 },
                ],
            },
            {
                field: ENTITY_FILTER_NAME,
                aggregations: [
                    { value: 'DASHBOARD', count: 5 },
                    { value: 'CHART', count: 100 },
                ],
            },
            {
                field: TAGS_FILTER_NAME,
                aggregations: [{ value: 'urn:li:tag:tag1', count: 20 }],
            },
        ],
    },
};

describe('getDisplayedFilterOptions', () => {
    const testEntityRegistry = getTestEntityRegistry();
    const mockSetSelectedFilterOptions = () => {};

    it('should get the list of displayed filter options with nested filters underneath their parents', () => {
        const filterOptions = getDisplayedFilterOptions(
            [],
            testEntityRegistry,
            mockSetSelectedFilterOptions,
            '',
            mockData,
        );

        const expectedFilterOptions = [
            {
                key: 'DATASET',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Datasets',
                nestedOptions: [
                    { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET␞table', count: 6 },
                    { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET␞view', count: 6 },
                ],
            },
            {
                key: 'CONTAINER',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'CONTAINER', count: 6 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Containers',
                nestedOptions: [{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'CONTAINER␞test', count: 3 }],
            },
            {
                key: 'DASHBOARD',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DASHBOARD', count: 5 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Dashboards',
                nestedOptions: [],
            },
            {
                key: 'CHART',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'CHART', count: 100 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Charts',
                nestedOptions: [],
            },
        ];

        expect(filterOptions).toMatchObject(expectedFilterOptions);
    });

    it('should get displayed filters and filter for nestedFilters', () => {
        const filterOptions = getDisplayedFilterOptions(
            [],
            testEntityRegistry,
            mockSetSelectedFilterOptions,
            'vie',
            mockData,
        );

        const expectedFilterOptions = [
            {
                key: 'DATASET',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Datasets',
                nestedOptions: [{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET␞view', count: 6 }],
            },
        ];

        expect(filterOptions).toMatchObject(expectedFilterOptions);
    });

    it('should get displayed filters and filter for top level filters', () => {
        const filterOptions = getDisplayedFilterOptions(
            [],
            testEntityRegistry,
            mockSetSelectedFilterOptions,
            'datase',
            mockData,
        );

        const expectedFilterOptions = [
            {
                key: 'DATASET',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Datasets',
                nestedOptions: [],
            },
        ];

        expect(filterOptions).toMatchObject(expectedFilterOptions);
    });
});

describe('getNumActiveFilters', () => {
    it('should get the number of active filters in a backwards compatible way', () => {
        const activeFilters = [
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASET␞view', 'DATASET␞table'] },
            { field: LEGACY_ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
            { field: ENTITY_FILTER_NAME, values: ['CONTAINER'] },
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
        ];
        const numActiveFilters = getNumActiveFilters(activeFilters);
        expect(numActiveFilters).toBe(4); // should not count duplicate values
    });
});

describe('getInitialSelectedOptions', () => {
    it('should get a list of options that are initially selected when opening the dropdown', () => {
        const activeFilters = [
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASET␞view', 'DATASET␞table', 'DATASET'] },
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
        ];
        const initialSelectedOptions = getInitialSelectedOptions(activeFilters, mockData);
        expect(initialSelectedOptions).toMatchObject([
            { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12, entity: undefined },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET␞table', count: 6, entity: undefined },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET␞view', count: 6, entity: undefined },
        ]);
    });

    it('should get a list of options that are initially selected when opening the dropdown with ENTITY_FILTER_NAME filter', () => {
        const activeFilters = [
            { field: ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
        ];
        const initialSelectedOptions = getInitialSelectedOptions(activeFilters, mockData);
        expect(initialSelectedOptions).toMatchObject([
            { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12, entity: undefined },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'CONTAINER', count: 6, entity: undefined },
        ]);
    });

    it('should get a list of options that are initially selected when opening the dropdown with LEGACY_ENTITY_FILTER_NAME filter', () => {
        const activeFilters = [
            { field: LEGACY_ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
        ];
        const initialSelectedOptions = getInitialSelectedOptions(activeFilters, mockData);
        expect(initialSelectedOptions).toMatchObject([
            { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12, entity: undefined },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'CONTAINER', count: 6, entity: undefined },
        ]);
    });
});

describe('getInitialSelectedOptionsFromAggregations', () => {
    const activeFilterValues = ['DATASET␞table', 'CONTAINER'];
    const aggregations = [
        { value: 'DATASET', count: 12, entity: null },
        { value: 'DATASET␞table', count: 6, entity: null },
        { value: 'DATASET␞view', count: 6, entity: null },
        { value: 'CONTAINER', count: 6, entity: null },
        { value: 'CONTAINER␞test', count: 3, entity: null },
    ];
    it('should get initially selected options from aggregations based on what is in activeFilters', () => {
        const initialOptions = getInitialSelectedOptionsFromAggregations(aggregations, activeFilterValues, []);
        expect(initialOptions).toMatchObject([
            {
                field: ENTITY_SUB_TYPE_FILTER_NAME,
                value: 'DATASET␞table',
                entity: null,
                count: 6,
            },
            {
                field: ENTITY_SUB_TYPE_FILTER_NAME,
                value: 'CONTAINER',
                entity: null,
                count: 6,
            },
        ]);
    });

    it('should get initially selected options from aggregations without duplicates', () => {
        const initialOptions = getInitialSelectedOptionsFromAggregations(aggregations, activeFilterValues, [
            {
                field: ENTITY_SUB_TYPE_FILTER_NAME,
                value: 'DATASET␞table',
                entity: null,
                count: 6,
            },
        ]);
        expect(initialOptions).toMatchObject([
            {
                field: ENTITY_SUB_TYPE_FILTER_NAME,
                value: 'CONTAINER',
                entity: null,
                count: 6,
            },
        ]);
    });
});
