import React, { useState } from 'react';
import { useEntityRegistry } from '../../../useEntityRegistry';
import OptionsDropdownMenu from '../OptionsDropdownMenu';
import { mapFilterOption } from '../mapFilterOption';
import { EntityFilterField, FilterValue, FilterValueOption } from '../types';
import { OptionMenu } from './styledComponents';
import { deduplicateOptions, useFilterOptionsBySearchQuery, useLoadSearchOptions } from './utils';

interface Props {
    field: EntityFilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    onApply: () => void;
    type?: 'card' | 'default';
    includeCount?: boolean;
    className?: string;
}

export default function EntityValueMenu({
    field,
    values,
    defaultOptions,
    type = 'card',
    includeCount = false,
    onChangeValues,
    onApply,
    className,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isSearchable = !!field.entityTypes?.length;
    const { displayName } = field;

    // Ideally we would not have staged values, and filters would update automatically.
    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    // Here we optionally load the search options, which are the options that are displayed when the user searches.
    const { options: searchOptions, loading: searchLoading } = useLoadSearchOptions(field, searchQuery, !isSearchable);

    const localSearchOptions = useFilterOptionsBySearchQuery(defaultOptions, searchQuery);

    const finalSearchOptions = [...localSearchOptions, ...deduplicateOptions(localSearchOptions, searchOptions)];

    // Compute the final options to show to the user.
    const finalDefaultOptions = defaultOptions.length ? defaultOptions : searchOptions;
    const finalOptions = searchQuery ? finalSearchOptions : finalDefaultOptions;

    // Finally, create the option set.
    // TODO: Add an option set for "no x".
    const filterMenuOptions = finalOptions.map((option) =>
        mapFilterOption({
            filterOption: {
                field: field.field,
                value: option.value,
                count: option.count,
                entity: option.entity,
                displayName: option.displayName,
            },
            entityRegistry,
            includeCount,
            selectedFilterOptions: values.map((value) => {
                return { field: field.field, value: value.value, entity: value.entity };
            }),
            setSelectedFilterOptions: (newOptions) =>
                onChangeValues(
                    newOptions.map((op) => {
                        return { field: field.field, value: op.value, entity: op.entity || null };
                    }),
                ),
        }),
    );

    return (
        <OptionsDropdownMenu
            menu={<OptionMenu items={filterMenuOptions} />}
            updateFilters={onApply}
            searchQuery={searchQuery || ''}
            updateSearchQuery={setSearchQuery}
            isLoading={searchLoading}
            searchPlaceholder={`Search for ${displayName}`}
            type={type}
            className={className}
        />
    );
}
