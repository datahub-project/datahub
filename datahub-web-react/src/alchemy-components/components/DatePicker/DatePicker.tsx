import React, { forwardRef, useEffect, useImperativeHandle, useMemo, useRef, useState } from 'react';

import { buildDateTimeFormat } from '@components/components/DatePicker/DatePicker.utils';
import { DatePickerGlobalStyles } from '@components/components/DatePicker/DatePickerGlobalStyles';
import { DatePickerWrapper, Label, StyledAntdDatePicker } from '@components/components/DatePicker/components';
import { DatePickerVariant } from '@components/components/DatePicker/constants';
import useVariantProps from '@components/components/DatePicker/hooks/useVariantProps';
import { DatePickerProps, DatePickerValue } from '@components/components/DatePicker/types';

const FOCUSABLE_SELECTOR = '[tabindex]:not([tabindex="-1"])';
const CONTENTS_STYLE: React.CSSProperties = { display: 'contents' };

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

    // showTime extends the format in play rather than replacing it.
    const resolvedFormat = showTime ? buildDateTimeFormat(format ?? datePickerProps.format) : format;

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

    // rc-picker 4 renders the custom input as a component type, so its identity must stay stable or the
    // input remounts (and loses focus) on every render. It reads the latest render function from a ref.
    const wrappedInputRenderRef = useRef(wrappedInputRender);
    wrappedInputRenderRef.current = wrappedInputRender;
    const StableInput = useMemo(
        () =>
            forwardRef<HTMLElement, React.InputHTMLAttributes<HTMLInputElement>>((props, ref) => {
                const holderRef = useRef<HTMLSpanElement>(null);
                // rc-picker focuses this ref on click and compares it with the mousedown target, so it has to
                // be the focusable element the variant rendered (the variants don't forward refs themselves).
                // The input wins over a focusable wrapper; the date switcher has no input, only a tabindex.
                useImperativeHandle(ref, () => {
                    const holder = holderRef.current;
                    return (holder?.querySelector<HTMLElement>('input') ??
                        holder?.querySelector<HTMLElement>(FOCUSABLE_SELECTOR) ??
                        holder) as HTMLElement;
                });
                return (
                    <span ref={holderRef} style={CONTENTS_STYLE}>
                        {wrappedInputRenderRef.current?.(props)}
                    </span>
                );
            }),
        [],
    );

    return (
        <>
            <DatePickerGlobalStyles />
            <DatePickerWrapper>
                {label && <Label aria-label={label}>{label}</Label>}
                <StyledAntdDatePicker
                    {...datePickerProps}
                    {...(resolvedFormat ? { format: resolvedFormat } : {})}
                    value={value}
                    components={wrappedInputRender ? { input: StableInput } : undefined}
                    onChange={(newValue) => setInternalValue(newValue as DatePickerValue)}
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
