import React, { useState } from 'react';
<<<<<<< HEAD

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { mapFilterOption } from '@app/searchV2/filters/mapFilterOption';
import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import { getFilterDisplayName, useFilterDisplayName } from '@app/searchV2/filters/utils';
import { OptionMenu } from '@app/searchV2/filters/value/styledComponents';
import {
    deduplicateOptions,
    useFilterOptionsBySearchQuery,
    useLoadAggregationOptions,
} from '@app/searchV2/filters/value/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType } from '@src/types.generated';
=======
import { FilterField, FilterValue, FilterValueOption } from '../types';
import { mapFilterOption } from '../mapFilterOption';
import { useEntityRegistry } from '../../../useEntityRegistry';
import OptionsDropdownMenu from '../OptionsDropdownMenu';
import { deduplicateOptions, useFilterOptionsBySearchQuery, useLoadAggregationOptions } from './utils';
import { OptionMenu } from './styledComponents';
import { getFilterDisplayName, useFilterDisplayName } from '../utils';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    onApply: () => void;
    type?: 'card' | 'default';
    includeCount?: boolean;
    className?: string;
    aggregationsEntityTypes?: Array<EntityType>;
}

export default function EnumValueMenu({
    field,
    values,
    includeCount = false,
    defaultOptions,
    type = 'card',
    onChangeValues,
    onApply,
    className,
    aggregationsEntityTypes,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = useFilterDisplayName(field);

    // Ideally we would not have staged values, and filters would update automatically.
    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    // Here we optionally load the aggregation options, which are the options that are displayed by default.
    const { options: aggOptions, loading: aggLoading } = useLoadAggregationOptions({
        field,
        visible: true,
        includeCounts: includeCount,
        aggregationsEntityTypes,
    });

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
<<<<<<< HEAD
                displayName: getFilterDisplayName(option, field, aggregationsEntityTypes),
=======
                displayName: getFilterDisplayName(option, field),
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
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
            className={className}
        />
    );
}
