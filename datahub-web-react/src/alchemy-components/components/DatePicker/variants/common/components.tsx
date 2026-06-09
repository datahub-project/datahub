import { Input } from '@components';
import { Calendar } from '@phosphor-icons/react/dist/csr/Calendar';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { ExtendedInputRenderProps } from '@components/components/DatePicker/types';

export function DefaultDatePickerInput({ datePickerProps, ...props }: ExtendedInputRenderProps) {
    const { t } = useTranslation('alchemy');
    const { disabled } = datePickerProps;
    return (
        <Input
            {...props}
            label=""
            value={props.value || ''}
            isDisabled={disabled}
            placeholder={props.placeholder || t('datePicker.placeholder')}
            icon={{ icon: Calendar }}
            isReadOnly
            style={{
                cursor: disabled ? 'not-allowed' : 'pointer',
                ...props.style,
            }}
        />
    );
}
