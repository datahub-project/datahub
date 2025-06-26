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
            });
    }, [disabled, isOpen, inputRender]);

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
