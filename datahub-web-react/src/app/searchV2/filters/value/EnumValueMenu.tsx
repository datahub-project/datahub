import React, { CSSProperties, useState } from 'react';
import { FilterField, FilterValue, FilterValueOption } from '../types';
import { mapFilterOption } from '../mapFilterOption';
import { useEntityRegistry } from '../../../useEntityRegistry';
import OptionsDropdownMenu from '../OptionsDropdownMenu';
import { deduplicateOptions, useFilterOptionsBySearchQuery, useLoadAggregationOptions } from './utils';
import { OptionMenu } from './styledComponents';

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

export default function EnumValueMenu({
    field,
    values,
    includeCount = false,
    style,
    defaultOptions,
    type = 'card',
    onChangeValues,
    onApply,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { displayName } = field;

    // Ideally we would not have staged values, and filters would update automatically.
    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    // Here we optionally load the aggregation options, which are the options that are displayed by default.
    const { options: aggOptions, loading: aggLoading } = useLoadAggregationOptions(field, true, includeCount);

    const allOptions = [...defaultOptions, ...deduplicateOptions(defaultOptions, aggOptions)];

    const localSearchOptions = useFilterOptionsBySearchQuery(allOptions, searchQuery);

    // Compute the final options to show to the user.
    const finalOptions = searchQuery ? localSearchOptions : allOptions;

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
                return { field: field.field, value: value.value };
            }),
            setSelectedFilterOptions: (newOptions) =>
                onChangeValues(
                    newOptions.map((op) => {
                        return { field: field.field, value: op.value, entity: null };
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
            isLoading={aggLoading}
            searchPlaceholder={displayName}
            type={type}
            style={style}
        />
    );
}
