import React, { useCallback, useEffect, useState } from 'react';

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
import { Tooltip } from '@components/components/Tooltip';

export const checkboxDefaults: CheckboxProps = {
    error: '',
    isChecked: false,
    isDisabled: false,
    isIntermediate: false,
    isRequired: false,
    setIsChecked: () => {},
    size: 'md',
    justifyContent: 'center',
};

export const Checkbox = ({
    label = checkboxDefaults.label,
    labelTooltip,
    error = checkboxDefaults.error,
    isChecked = checkboxDefaults.isChecked,
    isDisabled = checkboxDefaults.isDisabled,
    isIntermediate = checkboxDefaults.isIntermediate,
    isRequired = checkboxDefaults.isRequired,
    setIsChecked = checkboxDefaults.setIsChecked,
    size = checkboxDefaults.size,
    onCheckboxChange,
    dataTestId,
    justifyContent = checkboxDefaults.justifyContent,
    gap,
    shouldHandleLabelClicks,
    ...props
}: CheckboxProps) => {
    const [checked, setChecked] = useState(isChecked || false);

    useEffect(() => {
        setChecked(isChecked || false);
    }, [isChecked]);

    const id = props.id || `checkbox-${label}`;

    const onClick = useCallback(
        (e: React.MouseEvent) => {
            e.stopPropagation();
            e.preventDefault();
            if (!isDisabled) {
                setChecked(!checked);
                setIsChecked?.(!checked);
                onCheckboxChange?.(!checked);
            }
        },
        [setIsChecked, onCheckboxChange, checked, isDisabled],
    );

    return (
        <CheckboxContainer justifyContent={justifyContent} gap={gap}>
            {label ? (
                <Tooltip title={labelTooltip}>
                    <Label
                        aria-label={label}
                        clickable={shouldHandleLabelClicks}
                        onClick={shouldHandleLabelClicks ? onClick : undefined}
                    >
                        {label} {isRequired && <Required>*</Required>}
                    </Label>
                </Tooltip>
            ) : null}
            <CheckboxBase onClick={onClick} data-testid={dataTestId}>
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
