/* eslint-disable import/no-cycle */
import React, { useState } from 'react';

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { mapFilterOption } from '@app/searchV2/filters/mapFilterOption';
import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import { OptionList } from '@app/searchV2/filters/value/styledComponents';
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
    includeSubTypes?: boolean;
    includeCount?: boolean;
    className?: string;
    aggregationsEntityTypes?: Array<EntityType>;
    isRenderedInSubMenu?: boolean;
}

export default function EntityTypeMenu({
    field,
    values,
    defaultOptions,
    onChangeValues,
    includeSubTypes = true,
    includeCount = false,
    className,
    aggregationsEntityTypes,
    isRenderedInSubMenu,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { displayName } = field;

    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    const { options: aggOptions, loading: aggLoading } = useLoadAggregationOptions({
        field,
        visible: true,
        includeCounts: includeCount,
        aggregationsEntityTypes,
    });

    const allOptions = [...defaultOptions, ...deduplicateOptions(defaultOptions, aggOptions)];

    const localSearchOptions = useFilterOptionsBySearchQuery(allOptions, searchQuery);

    const finalOptions = searchQuery ? localSearchOptions : allOptions;

    const filterMenuOptions = finalOptions
        .filter((option) => !option.value.includes(FILTER_DELIMITER))
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
            menu={
                <OptionList>
                    {filterMenuOptions.map((opt) => (
                        <React.Fragment key={opt.key}>{opt.label}</React.Fragment>
                    ))}
                </OptionList>
            }
            searchQuery={searchQuery || ''}
            updateSearchQuery={setSearchQuery}
            isLoading={aggLoading}
            searchPlaceholder={displayName}
            className={className}
            isRenderedInSubMenu={isRenderedInSubMenu}
        />
    );
}
