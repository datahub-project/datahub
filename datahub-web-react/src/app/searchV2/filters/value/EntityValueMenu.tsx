import React, { CSSProperties, useState } from 'react';
import { useEntityRegistry } from '../../../useEntityRegistry';
import OptionsDropdownMenu from '../OptionsDropdownMenu';
import { mapFilterOption } from '../mapFilterOption';
import { FilterField, FilterValue, FilterValueOption } from '../types';
import { OptionMenu } from './styledComponents';
import {
    deduplicateOptions,
    mapFilterCountsToZero,
    useFilterOptionsBySearchQuery,
    useLoadAggregationOptions,
    useLoadSearchOptions,
} from './utils';

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    onApply: () => void;
    type?: 'card' | 'default';
    includeCount?: boolean;
    style?: CSSProperties;
}

export default function EntityValueMenu({
    field,
    values,
    defaultOptions,
    type = 'card',
    includeCount = false,
    style,
    onChangeValues,
    onApply,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isSearchable =
        field.entityTypes?.length && field.entityTypes.every((t) => entityRegistry.getEntity(t).isSearchEnabled());
    const { displayName } = field;

    // Ideally we would not have staged values, and filters would update automatically.
    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    // Here we optionally load the aggregation options, which are the options that are displayed by default.
    const { options: aggOptionsWithTooHighCounts, loading: aggLoading } = useLoadAggregationOptions(
        field,
        true,
        includeCount,
    );
    // Here we optionally load the search options, which are the options that are displayed when the user searches.
    const { options: searchOptions, loading: searchLoading } = useLoadSearchOptions(field, searchQuery, !isSearchable);

    // agg options are generated from a * query and their counts will be off as a result.
    const aggOptionsWithEmptyCounts = mapFilterCountsToZero(aggOptionsWithTooHighCounts);

    const allOptions = [...defaultOptions, ...deduplicateOptions(defaultOptions, aggOptionsWithEmptyCounts)];

    const localSearchOptions = useFilterOptionsBySearchQuery(allOptions, searchQuery);

    const finalSearchOptions = [...localSearchOptions, ...deduplicateOptions(localSearchOptions, searchOptions)];

    // Compute the final options to show to the user.
    const finalOptions = searchQuery ? finalSearchOptions : allOptions;

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
            isLoading={searchLoading || aggLoading}
            searchPlaceholder={`Search for ${displayName}`}
            type={type}
            style={style}
        />
    );
}
