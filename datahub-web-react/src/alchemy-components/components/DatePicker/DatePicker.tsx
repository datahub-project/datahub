import React, { useEffect, useMemo, useRef, useState } from 'react';

import { DatePickerGlobalStyles } from '@components/components/DatePicker/DatePickerGlobalStyles';
import { DatePickerWrapper, Label, StyledAntdDatePicker } from '@components/components/DatePicker/components';
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
    'data-testid': dataTestId,
    label,
    showTime,
    format,
}: DatePickerProps) {
    const [internalValue, setInternalValue] = useState<DatePickerValue | undefined>(value);

    const [isOpen, setIsOpen] = useState<boolean>(false);
    const presetProps = useVariantProps(variant);
    const { inputRender, ...datePickerProps } = presetProps;

    // When showTime is enabled, automatically use a time-inclusive format if not explicitly provided
    const finalFormat = format || (showTime ? 'YYYY-MM-DD HH:mm:ss' : undefined);

    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;
    useEffect(() => onChangeRef.current?.(internalValue), [internalValue]);

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
        <>
            <DatePickerGlobalStyles />
            <DatePickerWrapper>
                {label && <Label aria-label={label}>{label}</Label>}
                <StyledAntdDatePicker
                    {...datePickerProps}
                    {...(finalFormat ? { format: finalFormat } : {})}
                    value={value}
                    inputRender={wrappedInputRender && ((props) => wrappedInputRender?.(props))}
                    onChange={(newValue) => setInternalValue(newValue)}
                    onOpenChange={(open) => setIsOpen(open)}
                    disabled={disabled}
                    disabledDate={disabledDate}
                    showTime={showTime}
                    data-testid={dataTestId}
                />
            </DatePickerWrapper>
        </>
    );
}
