import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';

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

const DEBOUNCE_MS = 300;

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
    const { t } = useTranslation('search');
    const entityRegistry = useEntityRegistry();
    const isSearchable = !!field.entityTypes?.length;
    const { displayName } = field;

    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    // Debounce the value that drives the server-side autocomplete query so we don't fire a
    // GetAutoCompleteMultipleResults request on every keystroke. The raw searchQuery still
    // drives the input and local (client-side) option filtering for instant feedback.
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState<string | undefined>(undefined);
    useDebounce(() => setDebouncedSearchQuery(searchQuery), DEBOUNCE_MS, [searchQuery]);

    // Here we optionally load the search options, which are the options that are displayed when the user searches.
    const { options: searchOptions, loading: searchLoading } = useLoadSearchOptions(
        field,
        debouncedSearchQuery,
        !isSearchable,
    );

    const localSearchOptions = useFilterOptionsBySearchQuery(defaultOptions, searchQuery);

    // Only merge remote results once the debounced query is set. While debounce is pending,
    // useLoadSearchOptions still holds the initial '*' search; merging those would flash
    // unfiltered entities into the URN structured-property picker.
    const finalSearchOptions = [
        ...localSearchOptions,
        ...(debouncedSearchQuery ? deduplicateOptions(localSearchOptions, searchOptions) : []),
    ];

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
            searchPlaceholder={t('filters.searchFor', { name: displayName })}
            className={className}
            isRenderedInSubMenu={isRenderedInSubMenu}
        />
    );
}
