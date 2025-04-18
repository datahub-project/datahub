import React from 'react';
import { EntityRegistry } from '../../../entityRegistryContext';
import FilterOption from './FilterOption';
import { FilterOptionType } from './types';
import { getFilterIconAndLabel } from './utils';

export interface DisplayedFilterOption {
    key: string;
    label: React.ReactNode;
    style: any;
    displayName?: string | null;
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
        style: { padding: 0 },
        displayName: displayName as string,
        nestedOptions,
    };
}
