import { Tooltip } from '@components';
import React, { useEffect, useState } from 'react';
import { IconContainer, Label, Required, Slider, StyledIcon, StyledInput, SwitchContainer } from './components';
import { SwitchProps } from './types';

export const switchDefaults: SwitchProps = {
    label: 'Label',
    labelPosition: 'left',
    colorScheme: 'violet',
    size: 'md',
    isSquare: false,
    isChecked: false,
    isDisabled: false,
    isRequired: false,
};

export const Switch = ({
    label = switchDefaults.label,
    labelPosition = switchDefaults.labelPosition,
    icon, // undefined by default
    colorScheme = switchDefaults.colorScheme,
    size = switchDefaults.size,
    isSquare = switchDefaults.isSquare,
    isChecked = switchDefaults.isChecked,
    isDisabled = switchDefaults.isDisabled,
    isRequired = switchDefaults.isRequired,
    labelHoverText,
    disabledHoverText,
    labelStyle,
    ...props
}: SwitchProps) => {
    const [checked, setChecked] = useState(isChecked);

    useEffect(() => {
        setChecked(isChecked);
    }, [isChecked]);

    const id = props.id || `switchToggle-${label}`;

    return (
        <SwitchContainer labelPosition={labelPosition || 'left'} isDisabled={isDisabled}>
            <Tooltip title={labelHoverText} showArrow={false}>
                <Label id={id} aria-label={label} style={labelStyle}>
                    {label} {isRequired && <Required>*</Required>}
                </Label>
            </Tooltip>
            <StyledInput
                type="checkbox"
                checked={checked}
                onChange={() => setChecked(!checked)}
                customSize={size}
                disabled={isDisabled}
                colorScheme={colorScheme || 'violet'}
                aria-labelledby={id}
                aria-checked={checked}
                {...props}
            />
            <Tooltip title={isDisabled && disabledHoverText ? disabledHoverText : undefined} showArrow={false}>
                <Slider size={size} isSquare={isSquare} isDisabled={isDisabled}>
                    <IconContainer>
                        {icon && (
                            <StyledIcon
                                icon={icon}
                                color={checked ? colorScheme || 'violet' : 'inherit'}
                                size={size || 'md'}
                                checked={checked}
                            />
                        )}
                    </IconContainer>
                </Slider>
            </Tooltip>
        </SwitchContainer>
    );
};
