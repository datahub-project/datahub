import React from 'react';

import FilterOption from '@app/searchV2/filters/FilterOption';
import { FilterOptionType } from '@app/searchV2/filters/types';

type DisplayedFilterOption = {
    key: string;
    label: React.ReactNode;
    style: React.CSSProperties;
};

interface CreateFilterOptionProps {
    filterOption: FilterOptionType;
    selectedFilterOptions: FilterOptionType[];
    setSelectedFilterOptions: (values: FilterOptionType[]) => void;
    nestedOptions?: FilterOptionType[];
    includeCount?: boolean;
}

export function mapFilterOption({
    filterOption,
    selectedFilterOptions,
    setSelectedFilterOptions,
    nestedOptions,
    includeCount = true,
}: CreateFilterOptionProps): DisplayedFilterOption {
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
    };
}
