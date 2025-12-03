import React, { InputHTMLAttributes, forwardRef, useCallback } from 'react';

import { Checkbox } from '@components/components/Checkbox/Checkbox';

import { SizeOptions } from '@src/alchemy-components/theme/config';

// Define the native checkbox props by extending InputHTMLAttributes<HTMLInputElement>
interface NativeCheckboxProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
    /**
     * The label for the checkbox
     */
    label?: string;
    /**
     * Tooltip for the label
     */
    labelTooltip?: string;
    /**
     * Error message to display
     */
    error?: string;
    /**
     * Whether the checkbox is checked
     */
    checked?: boolean;
    /**
     * Whether the checkbox is disabled
     */
    disabled?: boolean;
    /**
     * Whether the checkbox is in indeterminate state
     */
    indeterminate?: boolean;
    /**
     * Whether the checkbox is required
     */
    required?: boolean;
    /**
     * Size of the checkbox
     */
    size?: SizeOptions;
    /**
     * Data test ID for testing
     */
    'data-testid'?: string;
    /**
     * Whether to handle label clicks
     */
    shouldHandleLabelClicks?: boolean;
    /**
     * Gap between label and checkbox
     */
    gap?: string;

    justifyContent?: 'center' | 'flex-start';
}

/**
 * NativeCheckbox is a wrapper around the Alchemy Checkbox component that provides
 * a native HTML checkbox API with onChange, checked, and other standard props
 */
export const NativeCheckbox = forwardRef<HTMLInputElement, NativeCheckboxProps>(
    ({
        label,
        labelTooltip,
        error,
        checked = false,
        disabled = false,
        indeterminate = false,
        required = false,
        size = 'md',
        'data-testid': dataTestId,
        shouldHandleLabelClicks,
        gap,
        onChange,
        id,
        name,
        value,
        ...props
    }) => {
        // Handle the change event using the native checkbox API
        const handleChange = useCallback(
            (isChecked: boolean) => {
                // Create a synthetic event to mimic native checkbox behavior
                const syntheticEvent = {
                    target: {
                        checked: isChecked,
                        name,
                        value,
                        type: 'checkbox',
                        disabled,
                    },
                } as React.ChangeEvent<HTMLInputElement>;

                onChange?.(syntheticEvent);
            },
            [onChange, name, value, disabled],
        );

        return (
            <Checkbox
                label={label}
                labelTooltip={labelTooltip}
                error={error}
                isChecked={checked}
                isDisabled={disabled}
                isIntermediate={indeterminate}
                isRequired={required}
                setIsChecked={handleChange}
                size={size}
                dataTestId={dataTestId}
                shouldHandleLabelClicks={shouldHandleLabelClicks}
                gap={gap}
                onCheckboxChange={handleChange}
                id={id}
                {...props}
            />
        );
    },
);

NativeCheckbox.displayName = 'NativeCheckbox';
