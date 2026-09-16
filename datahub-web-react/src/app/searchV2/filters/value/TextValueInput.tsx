import { Input } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

interface Props {
    name: string;
    value: string;
    onChangeValue: (newValue: string) => void;
}

export default function TextValueInput({ name, value, onChangeValue }: Props) {
    const { t } = useTranslation('search');
    return (
        <Input
            placeholder={t('filters.enterValue', { name: name.toLocaleLowerCase() })}
            data-testid="edit-text-input"
            onChange={(e) => onChangeValue(e.target.value)}
            value={value}
        />
    );
}
