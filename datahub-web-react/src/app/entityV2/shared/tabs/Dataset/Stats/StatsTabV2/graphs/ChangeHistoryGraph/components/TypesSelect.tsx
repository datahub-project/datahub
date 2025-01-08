import React from 'react';
import { SelectOption, SimpleSelect } from '@components';

type TypesSelectProps = {
    options: SelectOption[];
    values: string[];
    loading: boolean;
    onUpdate: (values: string[]) => void;
};

export default function TypesSelect({ options, values, loading, onUpdate }: TypesSelectProps) {
    return (
        <SimpleSelect
            placeholder="Change Type"
            selectLabelProps={{ variant: 'labeled', label: 'Change Type' }}
            options={options}
            values={values}
            onUpdate={onUpdate}
            width="full"
            showClear={false}
            isDisabled={loading}
            isMultiSelect
        />
    );
}
