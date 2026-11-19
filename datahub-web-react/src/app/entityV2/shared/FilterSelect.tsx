import capitalize from 'lodash/capitalize';
import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';

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
    placeholder?: string;
    width?: number | 'fit-content' | 'full';
    shouldDisplayConfirmationFooter?: boolean;
}

// NestedSelect compares values globally, so include the group for free-form filter names.
function getFilterOptionValue(category: string | undefined, name: string): string {
    return JSON.stringify([category, name]);
}

export const FilterSelect = ({
    filterOptions,
    onFilterChange,
    excludedCategories,
    initialSelectedOptions,
    placeholder,
    width,
    shouldDisplayConfirmationFooter,
}: FilterSelectProps) => {
    const { t: tc } = useTranslation('common.actions');
    const handleFilterChange = useCallback(
        (selectedValues: NestedSelectOption[]) => {
            const updatedFilters = selectedValues.map((option: NestedSelectOption) => {
                return filterOptions[option.parentValue!].find(
                    (filter) => getFilterOptionValue(option.parentValue, filter.name) === option.value,
                )!;
            });

            onFilterChange(updatedFilters);
        },
        [filterOptions, onFilterChange],
    );

    const options = useMemo((): NestedSelectOption[] => {
        const createOptions = (category: string, filters: FilterOption[]): NestedSelectOption[] => {
            const parentOption: NestedSelectOption = {
                value: category,
                label: capitalize(category),
                isParent: true,
            };

            const childOptions: NestedSelectOption[] = filters.map((filter) => ({
                value: getFilterOptionValue(category, filter.name),
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

    const initialValues = useMemo(
        () =>
            initialSelectedOptions?.map((option) => ({
                ...option,
                value: getFilterOptionValue(option.parentValue, option.value),
            })),
        [initialSelectedOptions],
    );

    return (
        <NestedSelect
            placeholder={placeholder || tc('filter')}
            options={options}
            initialValues={initialValues}
            onUpdate={handleFilterChange}
            isMultiSelect
            areParentsSelectable={false}
            width={width || 100}
            selectLabelProps={{ variant: 'labeled', label: placeholder || tc('filter') }}
            dataTestId="filter"
            shouldAlwaysSyncParentValues
            hideParentCheckbox
            shouldDisplayConfirmationFooter={shouldDisplayConfirmationFooter}
        />
    );
};
