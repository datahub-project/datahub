import React, { useMemo, useCallback } from 'react';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
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
    initialSelectedOptions?: NestedSelectOption[];
}

export const FilterSelect = ({
    filterOptions,
    onFilterChange,
    excludedCategories,
    initialSelectedOptions,
}: FilterSelectProps) => {
    const handleFilterChange = useCallback(
        (selectedValues: NestedSelectOption[]) => {
            const updatedFilters = selectedValues.map((option: NestedSelectOption) => {
                return filterOptions[option.parentValue!].find((filter) => filter.name === option.value)!;
            });

            onFilterChange(updatedFilters);
        },
        [filterOptions, onFilterChange],
    );

    const options = useMemo((): NestedSelectOption[] => {
        const createOptions = (category: string, filters: any[]): NestedSelectOption[] => {
            const parentOption: NestedSelectOption = {
                value: category,
                label: capitalize(category),
                isParent: true,
            };

            const childOptions: NestedSelectOption[] = filters.map((filter) => ({
                value: filter.name,
                label: filter.displayName,
                parentValue: category,
                isParent: false,
            }));

            return [parentOption, ...childOptions];
        };

        return Object.entries(filterOptions).reduce<NestedSelectOption[]>((opts, [category, filters]) => {
            if (!excludedCategories?.includes(category)) {
                opts.push(...createOptions(category, filters));
            }
            return opts;
        }, []);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filterOptions, excludedCategories]);

    return (
        <NestedSelect
            placeholder="Filter"
            options={options}
            initialValues={initialSelectedOptions}
            onUpdate={handleFilterChange}
            isMultiSelect
            areParentsSelectable={false}
            width={100}
            selectLabelProps={{ variant: 'labeled', label: 'Filter' }}
            shouldAlwaysSyncParentValues
            hideParentCheckbox
        />
    );
};
