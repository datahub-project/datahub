/* eslint-disable import/no-cycle */
import React, { useState } from 'react';

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { mapFilterOption } from '@app/searchV2/filters/mapFilterOption';
import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import { OptionMenu } from '@app/searchV2/filters/value/styledComponents';
import {
    deduplicateOptions,
    useFilterOptionsBySearchQuery,
    useLoadAggregationOptions,
} from '@app/searchV2/filters/value/utils';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType } from '@src/types.generated';

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    onApply: () => void;
    type?: 'card' | 'default';
    includeSubTypes?: boolean;
    includeCount?: boolean;
    className?: string;
    aggregationsEntityTypes?: Array<EntityType>;
}

export default function EntityTypeMenu({
    field,
    values,
    defaultOptions,
    type = 'card',
    onChangeValues,
    onApply,
    includeSubTypes = true,
    includeCount = false,
    className,
    aggregationsEntityTypes,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { displayName } = field;

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

    const filterMenuOptions = finalOptions
        .filter((option) => !option.value.includes(FILTER_DELIMITER)) // remove nested options
        .map((option) => {
            const nestedOptions = includeSubTypes
                ? aggOptions
                      .filter(
                          (aggOption) =>
                              aggOption.value.includes(FILTER_DELIMITER) && aggOption.value.includes(option.value),
                      )
                      .map((aggOption) => ({ field: ENTITY_SUB_TYPE_FILTER_NAME, ...aggOption }))
                : undefined;
            return mapFilterOption({
                filterOption: {
                    field: field.field,
                    value: option.value,
                    count: option.count,
                    entity: option.entity,
                    displayName: option.displayName,
                },
                includeCount,
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
                nestedOptions,
            });
        });

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
