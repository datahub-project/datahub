import { Input } from '@components';
import React from 'react';

import { FieldWrapper } from '@app/ingestV2/source/multiStepBuilder/components/FieldWrapper';

interface Props {
    label: string;
    value?: string;
    onChange?: (value: string) => void;
    name: string;
    required?: boolean;
    placeholder?: string;
}

export function Field({ label, value, onChange, name, required, placeholder }: Props) {
    return (
        <FieldWrapper label={label} required={required}>
            <Input
                label=""
                value={value}
                setValue={(newValue) => onChange?.(newValue)}
                name={name}
                placeholder={placeholder}
                isRequired={required}
            />
        </FieldWrapper>
    );
}
