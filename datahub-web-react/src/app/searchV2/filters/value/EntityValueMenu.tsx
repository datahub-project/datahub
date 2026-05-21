import React, { useState } from 'react';

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { mapFilterOption } from '@app/searchV2/filters/mapFilterOption';
import { EntityFilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import { OptionList } from '@app/searchV2/filters/value/styledComponents';
import {
    deduplicateOptions,
    useFilterOptionsBySearchQuery,
    useLoadSearchOptions,
} from '@app/searchV2/filters/value/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

interface Props {
    field: EntityFilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    includeCount?: boolean;
    className?: string;
    isRenderedInSubMenu?: boolean;
}

export default function EntityValueMenu({
    field,
    values,
    defaultOptions,
    includeCount = false,
    onChangeValues,
    className,
    isRenderedInSubMenu,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isSearchable = !!field.entityTypes?.length;
    const { displayName } = field;

    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    const { options: searchOptions, loading: searchLoading } = useLoadSearchOptions(field, searchQuery, !isSearchable);

    const localSearchOptions = useFilterOptionsBySearchQuery(defaultOptions, searchQuery);

    const finalSearchOptions = [...localSearchOptions, ...deduplicateOptions(localSearchOptions, searchOptions)];

    const finalDefaultOptions = defaultOptions.length ? defaultOptions : searchOptions;
    const finalOptions = searchQuery ? finalSearchOptions : finalDefaultOptions;

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
            menu={
                <OptionList>
                    {filterMenuOptions.map((opt) => (
                        <React.Fragment key={opt.key}>{opt.label}</React.Fragment>
                    ))}
                </OptionList>
            }
            searchQuery={searchQuery || ''}
            updateSearchQuery={setSearchQuery}
            isLoading={searchLoading}
            searchPlaceholder={`Search for ${displayName}`}
            className={className}
            isRenderedInSubMenu={isRenderedInSubMenu}
        />
    );
}
