import React from 'react';
import { dataPlatform, dataset1, glossaryTerm1 } from '../../../../Mocks';
import { EntityType } from '../../../../types.generated';
import { getTestEntityRegistry } from '../../../../utils/test-utils/TestPageContainer';
import { IconStyleType } from '../../../entity/Entity';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import {
    getFilterEntity,
    getNewFilters,
    isFilterOptionSelected,
    getFilterIconAndLabel,
    PlatformIcon,
    getNumActiveFiltersForFilter,
    getNumActiveFiltersForGroupOfFilters,
} from '../utils';

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
    it('should return true if the given filter value exists in the list', () => {
        const selectedFilterValues = ['one', 'two'];
        expect(isFilterOptionSelected(selectedFilterValues, 'two')).toBe(true);
    });

    it('should return false if the given filter value does not exist in the list', () => {
        const selectedFilterValues = ['one', 'two'];
        expect(isFilterOptionSelected(selectedFilterValues, 'testing123')).toBe(false);
    });

    it('should return false if the given filter value does not exist in the list, even if values are similar', () => {
        const selectedFilterValues = ['one', 'two'];
        expect(isFilterOptionSelected(selectedFilterValues, 'tw')).toBe(false);
    });
});

describe('filter utils - getFilterIconAndLabel', () => {
    const mockEntityRegistry = getTestEntityRegistry();

    it('should get the correct icon and label for entity filters', () => {
        const { icon, label } = getFilterIconAndLabel('entity', EntityType.Dataset, mockEntityRegistry, dataset1);

        expect(icon).toMatchObject(
            mockEntityRegistry.getIcon(EntityType.Dataset, 12, IconStyleType.ACCENT, ANTD_GRAY[9]),
        );
        expect(label).toBe(mockEntityRegistry.getCollectionName(EntityType.Dataset));
    });

    it('should get the correct icon and label for platform filters', () => {
        const { icon, label } = getFilterIconAndLabel('platform', dataPlatform.urn, mockEntityRegistry, dataPlatform);

        expect(icon).toMatchObject(<PlatformIcon src={dataPlatform.properties.logoUrl} />);
        expect(label).toBe(mockEntityRegistry.getDisplayName(EntityType.DataPlatform, dataPlatform));
    });

    it('should get the correct icon and label for filters with associated entity', () => {
        const { icon, label } = getFilterIconAndLabel('domains', glossaryTerm1.urn, mockEntityRegistry, glossaryTerm1);

        expect(icon).toMatchObject(
            mockEntityRegistry.getIcon(EntityType.GlossaryTerm, 12, IconStyleType.ACCENT, ANTD_GRAY[9]),
        );
        expect(label).toBe(mockEntityRegistry.getDisplayName(EntityType.GlossaryTerm, glossaryTerm1));
    });

    it('should get the correct icon and label for filters with no associated entity', () => {
        const { icon, label } = getFilterIconAndLabel('origin', 'PROD', mockEntityRegistry, null);

        expect(icon).toBe(null);
        expect(label).toBe('PROD');
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
