import React, { useState } from 'react';

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { mapFilterOption } from '@app/searchV2/filters/mapFilterOption';
import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import { getFilterDisplayName, useFilterDisplayName } from '@app/searchV2/filters/utils';
import { OptionList } from '@app/searchV2/filters/value/styledComponents';
import {
    deduplicateOptions,
    useFilterOptionsBySearchQuery,
    useLoadAggregationOptions,
} from '@app/searchV2/filters/value/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType } from '@src/types.generated';

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    includeCount?: boolean;
    className?: string;
    aggregationsEntityTypes?: Array<EntityType>;
    isRenderedInSubMenu?: boolean;
}

export default function EnumValueMenu({
    field,
    values,
    includeCount = false,
    defaultOptions,
    onChangeValues,
    className,
    aggregationsEntityTypes,
    isRenderedInSubMenu,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = useFilterDisplayName(field);

    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    const { options: aggOptions, loading: aggLoading } = useLoadAggregationOptions({
        field,
        visible: true,
        includeCounts: includeCount,
        aggregationsEntityTypes,
    });

    const optionsWithAggs = [...defaultOptions, ...deduplicateOptions(defaultOptions, aggOptions)];

    const { options: searchAggOptions, loading: searchAggsLoading } = useLoadAggregationOptions({
        field,
        visible: !!searchQuery,
        includeCounts: includeCount,
        aggregationsEntityTypes,
        extraOrFilters: [{ and: [{ field: field.field, values: [searchQuery || ''] }] }],
        removeOptionsWithNoCount: true,
    });

    const allOptions = [...optionsWithAggs, ...deduplicateOptions(optionsWithAggs, searchAggOptions)];

    const localSearchOptions = useFilterOptionsBySearchQuery(allOptions, searchQuery);

    const finalOptions = searchQuery ? localSearchOptions : allOptions;

    const filterMenuOptions = finalOptions.map((option) =>
        mapFilterOption({
            filterOption: {
                field: field.field,
                value: option.value,
                count: option.count,
                entity: option.entity,
                displayName: getFilterDisplayName(option, field),
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
            menu={
                <OptionList>
                    {filterMenuOptions.map((opt) => (
                        <React.Fragment key={opt.key}>{opt.label}</React.Fragment>
                    ))}
                </OptionList>
            }
            searchQuery={searchQuery || ''}
            updateSearchQuery={setSearchQuery}
            isLoading={aggLoading || searchAggsLoading}
            searchPlaceholder={displayName}
            className={className}
            isRenderedInSubMenu={isRenderedInSubMenu}
        />
    );
}
