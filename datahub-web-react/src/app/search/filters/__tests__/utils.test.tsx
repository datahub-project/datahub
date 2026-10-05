import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import {
    PlatformIcon,
    getFilterEntity,
    getFilterIconAndLabel,
    getNewFilters,
    isAnyOptionSelected,
    isFilterOptionSelected,
} from '@app/search/filters/utils';
import { dataPlatform, dataPlatformInstance, dataset1, glossaryTerm1 } from '@src/Mocks';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

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

        expect(icon).toMatchObject(<Folder weight="fill" color="black" />);
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

        expect(icon).toMatchObject(<Folder size={12} weight="fill" color="black" />);
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
