import { Input } from '@components';
import React from 'react';

import { ExtendedInputRenderProps } from '@components/components/DatePicker/types';

export function DefaultDatePickerInput({ datePickerProps, ...props }: ExtendedInputRenderProps) {
    const { disabled } = datePickerProps;
    return (
        <Input
            {...props}
            label=""
            value={props.value || ''}
            isDisabled={disabled}
            placeholder={props.placeholder || 'Select date'}
            icon={{ icon: 'CalendarMonth' }}
            isReadOnly
            style={{
                cursor: disabled ? 'not-allowed' : 'pointer',
                ...props.style,
            }}
        />
    );
}
