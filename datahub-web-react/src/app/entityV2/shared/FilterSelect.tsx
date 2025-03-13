import React, { useMemo, useCallback } from 'react';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import capitalize from 'lodash/capitalize';

interface FilterOption {
    name: string;
    category: string;
    count: number;
    displayName: string;
}

interface FilterGroupOptions {
    [key: string]: FilterOption[];
}

interface FilterSelectProps {
    filterOptions: FilterGroupOptions;
    onFilterChange: (selectedFilters: FilterOption[]) => void;
    excludedCategories?: string[];
    initialSelectedOptions?: SelectOption[];
}

export const FilterSelect = ({
    filterOptions,
    onFilterChange,
    excludedCategories,
    initialSelectedOptions,
}: FilterSelectProps) => {
    const handleFilterChange = useCallback(
        (selectedValues: SelectOption[]) => {
            const updatedFilters = selectedValues.map((option: SelectOption) => {
                return filterOptions[option.parentValue!].find((filter) => filter.name === option.value)!;
            });

            onFilterChange(updatedFilters);
        },
        [filterOptions, onFilterChange],
    );

    const options = useMemo((): SelectOption[] => {
        const createOptions = (category: string, filters: any[]): SelectOption[] => {
            const parentOption: SelectOption = {
                value: category,
                label: capitalize(category),
                isParent: true,
            };

            const childOptions: SelectOption[] = filters.map((filter) => ({
                value: filter.name,
                label: filter.displayName,
                parentValue: category,
                isParent: false,
            }));

            return [parentOption, ...childOptions];
        };

        return Object.entries(filterOptions).reduce<SelectOption[]>((opts, [category, filters]) => {
            if (!excludedCategories?.includes(category)) {
                opts.push(...createOptions(category, filters));
            }
            return opts;
        }, []);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filterOptions, excludedCategories]);

    return (
        <NestedSelect
            label=""
            placeholder="Filter"
            options={options}
            initialValues={initialSelectedOptions}
            onUpdate={handleFilterChange}
            isMultiSelect
            areParentsSelectable={false}
            width={100}
            showCount
            shouldAlwaysSyncParentValues
            hideParentCheckbox
        />
    );
};
