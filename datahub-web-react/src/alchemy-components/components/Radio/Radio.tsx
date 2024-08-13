import React, { useEffect, useState } from 'react';
import { RadioGroupProps, RadioProps } from './types';
import {
    RadioWrapper,
    Checkmark,
    HiddenInput,
    Label,
    Required,
    RadioLabel,
    RadioBase,
    RadioGroupContainer,
} from './components';

export const radioDefaults = {
    label: 'Label',
    error: '',
    isChecked: false,
    isDisabled: false,
    isRequired: false,
    isVertical: false,
    setIsChecked: () => {},
};

export const Radio = ({
    label = radioDefaults.label,
    error = radioDefaults.error,
    isChecked = radioDefaults.isChecked,
    isDisabled = radioDefaults.isDisabled,
    isRequired = radioDefaults.isRequired,
    setIsChecked = radioDefaults.setIsChecked,
    ...props
}: RadioProps) => {
    const [checked, setChecked] = useState(isChecked || false);

    useEffect(() => {
        setChecked(isChecked || false);
    }, [isChecked]);

    const id = props.id || `checkbox-${label}`;

    return (
        <RadioWrapper disabled={isDisabled} error={error}>
            <RadioBase>
                <HiddenInput
                    type="radio"
                    id={label}
                    value={label}
                    checked={checked}
                    disabled={isDisabled}
                    onChange={() => {
                        setChecked(true);
                        setIsChecked?.(true);
                    }}
                    aria-label={label}
                    aria-labelledby={id}
                    aria-checked={checked}
                    {...props}
                />
                <Checkmark checked={checked} disabled={isDisabled} error={error} />
            </RadioBase>
            {label && (
                <RadioLabel>
                    <Label onClick={() => setChecked(true)}>
                        {label} {isRequired && <Required>*</Required>}
                    </Label>
                </RadioLabel>
            )}
        </RadioWrapper>
    );
};

export const RadioGroup = ({ isVertical, radios }: RadioGroupProps) => {
    if (!radios.length) {
        return <></>;
    }

    return (
        <RadioGroupContainer isVertical={isVertical}>
            {radios.map((checkbox) => {
                const props = { ...checkbox };
                return (
                    <React.Fragment key={checkbox.label}>
                        <Radio {...props} />
                    </React.Fragment>
                );
            })}
        </RadioGroupContainer>
    );
};
