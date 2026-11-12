import { Input } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

interface Props {
    name: string;
    value: string;
    onChangeValue: (newValue: string) => void;
    onSubmit?: () => void;
}

export default function TextValueInput({ name, value, onChangeValue, onSubmit }: Props) {
    const { t } = useTranslation('search');
    return (
        <Input
            label=""
            placeholder={t('filters.enterValue', { name: name.toLocaleLowerCase() })}
            inputTestId="edit-text-input"
            setValue={onChangeValue}
            value={value}
            onKeyDown={(e) => {
                if (e.key !== 'Enter' || !onSubmit) return;
                // Keep the Enter away from an enclosing antd menu, which would treat it as an item click.
                e.stopPropagation();
                onSubmit();
            }}
        />
    );
}
