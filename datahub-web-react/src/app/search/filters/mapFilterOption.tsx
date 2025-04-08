import React from 'react';

import EntityRegistry from '@app/entity/EntityRegistry';
import FilterOption from '@app/search/filters/FilterOption';
import { FilterOptionType } from '@app/search/filters/types';
import { getFilterIconAndLabel } from '@app/search/filters/utils';

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
}

export function mapFilterOption({
    filterOption,
    entityRegistry,
    selectedFilterOptions,
    setSelectedFilterOptions,
    nestedOptions,
}: CreateFilterOptionProps): DisplayedFilterOption {
    const { label: displayName } = getFilterIconAndLabel(
        filterOption.field,
        filterOption.value,
        entityRegistry,
        filterOption.entity || null,
    );

    return {
        key: filterOption.value,
        label: (
            <FilterOption
                filterOption={filterOption}
                selectedFilterOptions={selectedFilterOptions}
                setSelectedFilterOptions={setSelectedFilterOptions}
                nestedOptions={nestedOptions}
            />
        ),
        style: { padding: 0 },
        displayName: filterOption.displayName || (displayName as string),
        nestedOptions,
    };
}
