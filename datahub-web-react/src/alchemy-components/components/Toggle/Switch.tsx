import React, { useEffect, useState } from 'react';
import { SwitchProps } from './types';
import { Label, Required, StyledInput, Slider, SwitchContainer, StyledIcon, IconContainer } from './components';

export const switchDefaults: SwitchProps = {
    label: 'Label',
    colorScheme: 'violet',
    size: 'md',
    isSquare: false,
    isChecked: false,
    isDisabled: false,
    isIntermediate: false,
    isRequired: false,
};

export const Switch = ({
    label = switchDefaults.label,
    icon, // undefined by default
    colorScheme = switchDefaults.colorScheme,
    size = switchDefaults.size,
    isSquare = switchDefaults.isSquare,
    isChecked = switchDefaults.isChecked,
    isDisabled = switchDefaults.isDisabled,
    isRequired = switchDefaults.isRequired,
    ...props
}: SwitchProps) => {
    const [checked, setChecked] = useState(isChecked);

    useEffect(() => {
        setChecked(isChecked);
    }, [isChecked]);

    return (
        <SwitchContainer>
            <Label>
                {label} {isRequired && <Required>*</Required>}
            </Label>
            <StyledInput
                type="checkbox"
                checked={checked}
                onChange={() => setChecked(!checked)}
                customSize={size}
                disabled={isDisabled}
                colorScheme={colorScheme || 'violet'}
                {...props}
            />
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
        </SwitchContainer>
    );
};
