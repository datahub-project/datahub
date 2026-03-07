import React from 'react';

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { mapFilterOption } from '@app/searchV2/filters/mapFilterOption';
import { FilterField, FilterValue } from '@app/searchV2/filters/types';
import { OptionList } from '@app/searchV2/filters/value/styledComponents';
import { useEntityRegistry } from '@app/useEntityRegistry';

const OPTIONS = [
    { value: 'true', count: undefined, entity: null },
    { value: 'false', count: undefined, entity: null },
];

interface Props {
    field: FilterField;
    values: FilterValue[];
    onChangeValues: (newValues: FilterValue[]) => void;
    className?: string;
}

export default function BooleanValueMenu({ field, values, onChangeValues, className }: Props) {
    const entityRegistry = useEntityRegistry();

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
            menu={
                <OptionList>
                    {filterMenuOptions.map((opt) => (
                        <React.Fragment key={opt.key}>{opt.label}</React.Fragment>
                    ))}
                </OptionList>
            }
            showSearchBar={false}
            className={className}
        />
    );
}
