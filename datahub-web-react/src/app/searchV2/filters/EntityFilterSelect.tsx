import { SimpleSelect } from '@components';
import React, { useCallback, useMemo } from 'react';
import { useDebounce } from 'react-use';

import { SelectOption } from '@components/components/Select/types';

import { EntitySelectOption } from '@app/entityV2/shared/components/select/EntitySelectOption';
import { EntityFilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import { getFilterIconAndLabel } from '@app/searchV2/filters/utils';
import {
    deduplicateOptions,
    useFilterOptionsBySearchQuery,
    useLoadSearchOptions,
} from '@app/searchV2/filters/value/utils';
import { DOMAINS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType } from '@src/types.generated';

const DEBOUNCE_SEARCH_MS = 300;

interface EntitySelectOptionType extends SelectOption {
    value: string;
    entity: FilterValueOption['entity'];
    counter?: number;
}

interface Props {
    field: EntityFilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    includeCount?: boolean;
    displayName: string;
}

export default function EntityFilterSelect({
    field,
    values,
    defaultOptions,
    onChangeValues,
    includeCount = false,
    displayName,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isSearchable = !!field.entityTypes?.length;

    const [searchInput, setSearchInput] = React.useState<string | undefined>(undefined);
    const [searchQuery, setSearchQuery] = React.useState<string | undefined>(undefined);

    useDebounce(() => setSearchQuery(searchInput), DEBOUNCE_SEARCH_MS, [searchInput]);

    const { options: searchOptions, loading: searchLoading } = useLoadSearchOptions(field, searchQuery, !isSearchable);

    const localSearchOptions = useFilterOptionsBySearchQuery(defaultOptions, searchQuery);

    const finalSearchOptions = [...localSearchOptions, ...deduplicateOptions(localSearchOptions, searchOptions)];

    const finalDefaultOptions = defaultOptions.length ? defaultOptions : searchOptions;
    const finalOptions = searchQuery ? finalSearchOptions : finalDefaultOptions;

    const selectedValues = useMemo(() => values.map((value) => value.value), [values]);

    const options: EntitySelectOptionType[] = useMemo(() => {
        return finalOptions.map((option) => {
            const { icon, label } = getFilterIconAndLabel(
                field.field,
                option.value,
                entityRegistry,
                option.entity || null,
                16,
                option.displayName,
            );

            const countText = includeCount && option.count !== undefined ? ` (${option.count})` : '';

            return {
                value: option.value,
                label: `${label}${countText}`,
                icon: icon || undefined,
                entity: option.entity,
                counter: includeCount ? option.count : undefined,
            };
        });
    }, [finalOptions, field.field, entityRegistry, includeCount]);

    const handleUpdate = useCallback(
        (newValues: string[]) => {
            onChangeValues(
                newValues.map((value) => {
                    const option = options.find((opt) => opt.value === value);
                    return { field: field.field, value, entity: option?.entity || null };
                }),
            );
        },
        [onChangeValues, options, field],
    );

    const handleClear = useCallback(() => {
        onChangeValues([]);
    }, [onChangeValues]);

    const handleSearchChange = useCallback((query: string) => {
        setSearchInput(query || undefined);
    }, []);

    const renderOptionText = useCallback(
        (option: EntitySelectOptionType) => {
            if (option.entity) {
                const showParentEntityPath = !(
                    field.field === DOMAINS_FILTER_NAME && option.entity?.type === EntityType.Domain
                );
                return (
                    <EntitySelectOption
                        entity={option.entity}
                        counter={option.counter}
                        showParentEntityPath={showParentEntityPath}
                    />
                );
            }

            // Default rendering with icon
            return (
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    {option.icon}
                    <span>{option.label}</span>
                </div>
            );
        },
        [field],
    );

    return (
        <SimpleSelect<EntitySelectOptionType>
            options={options}
            values={selectedValues}
            onUpdate={handleUpdate}
            onClear={handleClear}
            isActive={selectedValues.length > 0}
            isMultiSelect
            showSearch
            showClear
            onSearchChange={handleSearchChange}
            filterResultsByQuery={false}
            isLoading={searchLoading}
            placeholder={`Search for ${displayName}`}
            width="fit-content"
            selectLabelProps={{ variant: 'labeled', label: displayName }}
            renderCustomOptionText={renderOptionText}
            autocommit={false}
            dataTestId={`filter-dropdown-${displayName?.replace(/\s/g, '-')}`}
            shouldOrderSelectedOptionsToTop
            shouldUpdateValuesOnClose
        />
    );
}
