import React, { useState } from 'react';

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { mapFilterOption } from '@app/searchV2/filters/mapFilterOption';
import { FilterField, FilterValue } from '@app/searchV2/filters/types';
import { OptionMenu } from '@app/searchV2/filters/value/styledComponents';
import { useEntityRegistry } from '@app/useEntityRegistry';

// Since we are working with a boolean field, always simply have the base options.
const OPTIONS = [
    { value: 'true', count: undefined, entity: null },
    { value: 'false', count: undefined, entity: null },
];

interface Props {
    field: FilterField;
    values: FilterValue[];
    onChangeValues: (newValues: FilterValue[]) => void;
    onApply: () => void;
    type?: 'card' | 'default';
    className?: string;
}

export default function BooleanValueMenu({ field, values, type = 'card', onChangeValues, onApply, className }: Props) {
    const entityRegistry = useEntityRegistry();

    // Ideally we would not have staged values, and filters would update automatically.
    const [searchQuery, setSearchQuery] = useState<string | undefined>(undefined);

    const filterMenuOptions = OPTIONS.map((option) =>
        mapFilterOption({
            filterOption: {
                field: field.field,
                value: option.value,
                count: option.count,
                entity: option.entity,
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
            isLoading={false}
            showSearchBar={false}
            type={type}
            className={className}
        />
    );
}
