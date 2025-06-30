import React, { useEffect, useState } from 'react';

import {
    CheckboxBase,
    CheckboxContainer,
    CheckboxGroupContainer,
    Checkmark,
    Label,
    Required,
    StyledCheckbox,
} from '@components/components/Checkbox/components';
import { CheckboxGroupProps, CheckboxProps } from '@components/components/Checkbox/types';

export const checkboxDefaults: CheckboxProps = {
    error: '',
    isChecked: false,
    isDisabled: false,
    isIntermediate: false,
    isRequired: false,
    setIsChecked: () => {},
    size: 'md',
};

export const Checkbox = ({
    label = checkboxDefaults.label,
    error = checkboxDefaults.error,
    isChecked = checkboxDefaults.isChecked,
    isDisabled = checkboxDefaults.isDisabled,
    isIntermediate = checkboxDefaults.isIntermediate,
    isRequired = checkboxDefaults.isRequired,
    setIsChecked = checkboxDefaults.setIsChecked,
    size = checkboxDefaults.size,
    onCheckboxChange,
    ...props
}: CheckboxProps) => {
    const [checked, setChecked] = useState(isChecked || false);

    useEffect(() => {
        setChecked(isChecked || false);
    }, [isChecked]);

    const id = props.id || `checkbox-${label}`;

    return (
        <CheckboxContainer>
            {label ? (
                <Label aria-label={label}>
                    {label} {isRequired && <Required>*</Required>}
                </Label>
            ) : null}
            <CheckboxBase
                onClick={() => {
                    if (!isDisabled) {
                        setChecked(!checked);
                        setIsChecked?.(!checked);
                        onCheckboxChange?.();
                    }
                }}
            >
                <StyledCheckbox
                    type="checkbox"
                    id="checked-input"
                    checked={checked || isIntermediate || false}
                    disabled={isDisabled || false}
                    error={error || ''}
                    onChange={() => null}
                    aria-labelledby={id}
                    aria-checked={checked}
                    {...props}
                />
                <Checkmark
                    intermediate={isIntermediate || false}
                    error={error || ''}
                    disabled={isDisabled || false}
                    checked={checked || false}
                    size={size || 'md'}
                />
            </CheckboxBase>
        </CheckboxContainer>
    );
};

export const CheckboxGroup = ({ isVertical, checkboxes }: CheckboxGroupProps) => {
    if (!checkboxes.length) {
        return <></>;
    }

    return (
        <CheckboxGroupContainer isVertical={isVertical}>
            {checkboxes.map((checkbox) => {
                const props = { ...checkbox };
                return (
                    <React.Fragment key={checkbox.label}>
                        <Checkbox {...props} />
                    </React.Fragment>
                );
            })}
        </CheckboxGroupContainer>
    );
};
