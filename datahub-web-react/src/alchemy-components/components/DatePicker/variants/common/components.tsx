/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
