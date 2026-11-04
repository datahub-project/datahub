import { Input } from '@components';
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
            inputTestId="edit-text-input"
            setValue={onChangeValue}
            value={value}
        />
    );
}
