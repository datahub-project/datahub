/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useMemo, useState } from 'react';

import { StyledAntdDatePicker } from '@components/components/DatePicker/components';
import { DatePickerVariant } from '@components/components/DatePicker/constants';
import useVariantProps from '@components/components/DatePicker/hooks/useVariantProps';
import { DatePickerProps, DatePickerValue } from '@components/components/DatePicker/types';

export const datePickerDefault: DatePickerProps = {
    variant: DatePickerVariant.Default,
    disabled: false,
};

export function DatePicker({
    value,
    onChange,
    variant = datePickerDefault.variant,
    disabled = datePickerDefault.disabled,
    disabledDate,
    placeholder,
}: DatePickerProps) {
    const [internalValue, setInternalValue] = useState<DatePickerValue | undefined>(value);

    const [isOpen, setIsOpen] = useState<boolean>(false);
    const presetProps = useVariantProps(variant);
    const { inputRender, ...datePickerProps } = presetProps;

    useEffect(() => onChange?.(internalValue), [onChange, internalValue]);

    const wrappedInputRender = useMemo(() => {
        if (!inputRender) return undefined;

        return (props: React.InputHTMLAttributes<HTMLInputElement>) =>
            inputRender({
                ...props,
                datePickerProps: {
                    disabled,
                },
                datePickerState: {
                    open: isOpen,
                    setValue: setInternalValue,
                },
                placeholder,
            });
    }, [disabled, placeholder, isOpen, inputRender]);

    return (
        <StyledAntdDatePicker
            {...datePickerProps}
            value={value}
            inputRender={wrappedInputRender && ((props) => wrappedInputRender?.(props))}
            onChange={(newValue) => setInternalValue(newValue)}
            onOpenChange={(open) => setIsOpen(open)}
            disabled={disabled}
            disabledDate={disabledDate}
        />
    );
}
