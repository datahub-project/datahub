import React from 'react';

import FilterOption from '@app/searchV2/filters/FilterOption';
import { FilterOptionType } from '@app/searchV2/filters/types';
import { getFilterIconAndLabel } from '@app/searchV2/filters/utils';
import { EntityRegistry } from '@src/entityRegistryContext';

import { Entity } from '@types';

interface DisplayedFilterOption {
    key: string;
    label: React.ReactNode;
    displayName?: string | null;
    entity?: Entity | null;
    nestedOptions?: FilterOptionType[];
}

interface CreateFilterOptionProps {
    filterOption: FilterOptionType;
    entityRegistry: EntityRegistry;
    selectedFilterOptions: FilterOptionType[];
    setSelectedFilterOptions: (values: FilterOptionType[]) => void;
    nestedOptions?: FilterOptionType[];
    includeCount?: boolean;
}

export function mapFilterOption({
    filterOption,
    entityRegistry,
    selectedFilterOptions,
    setSelectedFilterOptions,
    nestedOptions,
    includeCount = true,
}: CreateFilterOptionProps): DisplayedFilterOption {
    const { label: displayName } = getFilterIconAndLabel(
        filterOption.field,
        filterOption.value,
        entityRegistry,
        filterOption.entity || null,
        undefined,
        filterOption.displayName,
    );

    return {
        key: filterOption.value,
        label: (
            <FilterOption
                filterOption={filterOption}
                selectedFilterOptions={selectedFilterOptions}
                setSelectedFilterOptions={setSelectedFilterOptions}
                nestedOptions={nestedOptions}
                includeCount={includeCount}
            />
        ),
        entity: filterOption.entity,
        displayName: displayName as string,
        nestedOptions,
    };
}
