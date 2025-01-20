import React from 'react';
import { SelectOption, SimpleSelect } from '@src/alchemy-components';

type UsersSelectProps = {
    options: SelectOption[];
    values: string[];
    onUpdate: (selectedValues: string[]) => void;
    isDisabled: boolean;
};

export default function UsersSelect({ options, values, onUpdate, isDisabled }: UsersSelectProps) {
    return (
        <SimpleSelect
            selectLabelProps={{ variant: 'labeled', label: 'Users' }}
            values={values}
            options={options}
            onUpdate={onUpdate}
            width="full"
            isDisabled={isDisabled}
            showClear={false}
            isMultiSelect
        />
    );
}
