import { Check, Minus } from '@phosphor-icons/react';
import React, { useCallback, useEffect, useState } from 'react';

import {
    CheckboxBase,
    CheckboxBox,
    CheckboxContainer,
    CheckboxGroupContainer,
    HiddenInput,
    Label,
    Required,
} from '@components/components/Checkbox/components';
import { CheckboxGroupProps, CheckboxProps } from '@components/components/Checkbox/types';
import { getCheckIconSize } from '@components/components/Checkbox/utils';
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

    const isActive = checked || isIntermediate || false;
    const iconSize = getCheckIconSize(size || 'md');

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
            <CheckboxBase onClick={onClick} data-testid={dataTestId} data-disabled={isDisabled}>
                <HiddenInput
                    type="checkbox"
                    checked={isActive}
                    disabled={isDisabled || false}
                    onChange={() => null}
                    aria-labelledby={id}
                    aria-checked={checked}
                    {...props}
                />
                <CheckboxBox $checked={isActive} $error={!!error} $disabled={isDisabled || false} $size={size || 'md'}>
                    {isIntermediate && <Minus size={iconSize} weight="bold" />}
                    {!isIntermediate && checked && <Check size={iconSize} weight="bold" />}
                </CheckboxBox>
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
