import React, { useMemo, useCallback } from 'react';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import capitalize from 'lodash/capitalize';
import { ASSERTION_FILTER_TYPES } from './constant';

interface FilterOption {
    name: string;
    category: string;
    count: number;
    displayName: string;
}

interface FilterGroupOptions {
    [key: string]: FilterOption[];
}

interface AcrylAssertionFiltersProps {
    filterOptions: FilterGroupOptions;
    selectedFilters: FilterOption[];
    onFilterChange: (selectedFilters: FilterOption[]) => void;
}

export const AcrylAssertionFilters: React.FC<AcrylAssertionFiltersProps> = ({
    filterOptions,
    selectedFilters,
    onFilterChange,
}) => {
    const initialSelectedOptions = useMemo(() => {
        return selectedFilters.map((filter) => ({
            value: filter.name,
            label: filter.displayName,
            parentValue: filter.category,
        }));
    }, [selectedFilters]);

    const handleFilterChange = useCallback(
        (selectedValues: SelectOption[]) => {
            const updatedFilters = selectedValues.map((option) => {
                const category = option.parentValue || '';
                return filterOptions[category].find((filter) => filter.name === option.value)!;
            });

            onFilterChange(updatedFilters);
        },
        [filterOptions, onFilterChange],
    );

    const getOptions = useMemo((): SelectOption[] => {
        const opts: SelectOption[] = [];
        Object.entries(filterOptions).forEach(([category, filters]) => {
            if (category !== ASSERTION_FILTER_TYPES.TAG) {
                opts.push({
                    value: category,
                    label: capitalize(category),
                    isParent: true,
                });
                opts.push(
                    ...filters.map((filter) => ({
                        value: filter.name,
                        label: filter.displayName,
                        parentValue: category,
                        isParent: false,
                    })),
                );
            }
        });
        return opts;
    }, [filterOptions]);

    return (
        <NestedSelect
            label=""
            placeholder="Filter"
            options={getOptions}
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
