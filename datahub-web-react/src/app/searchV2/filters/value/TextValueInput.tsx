import { Input } from 'antd';
import React from 'react';

interface Props {
    name: string;
    value: string;
    onChangeValue: (newValue: string) => void;
}

export default function TextValueInput({ name, value, onChangeValue }: Props) {
    return (
        <Input
            placeholder={`Enter ${name.toLocaleLowerCase()}`}
            data-testid="edit-text-input"
            onChange={(e) => onChangeValue(e.target.value)}
            value={value}
        />
    );
}
