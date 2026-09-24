import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { v4 as uuidv4 } from 'uuid';

import {
    Checkmark,
    HiddenInput,
    Label,
    RadioBase,
    RadioGroupContainer,
    RadioLabel,
    RadioWrapper,
    Required,
} from '@components/components/Radio/components';
import { RadioGroupProps, RadioProps } from '@components/components/Radio/types';

export const radioDefaults = {
    // Storybook-only default; the component resolves an unset label to t('radio.label') at render time.
    label: 'Label',
    error: '',
    isChecked: false,
    isDisabled: false,
    isRequired: false,
    isVertical: false,
    setIsChecked: () => {},
};

export const Radio = ({
    label,
    error = radioDefaults.error,
    isChecked = radioDefaults.isChecked,
    isDisabled = radioDefaults.isDisabled,
    isRequired = radioDefaults.isRequired,
    setIsChecked = radioDefaults.setIsChecked,
    ...props
}: RadioProps) => {
    const { t } = useTranslation('alchemy');
    const resolvedLabel = label ?? t('radio.label');
    const [checked, setChecked] = useState(isChecked || false);
    const [generatedId] = useState(() => `radio-${uuidv4()}`);

    useEffect(() => {
        setChecked(isChecked || false);
    }, [isChecked]);

    const id = props.id || generatedId;

    return (
        <RadioWrapper disabled={isDisabled}>
            <RadioBase disabled={isDisabled} error={error}>
                <HiddenInput
                    type="radio"
                    value={resolvedLabel}
                    checked={checked}
                    disabled={isDisabled}
                    onChange={() => {
                        setChecked(true);
                        setIsChecked?.(true);
                    }}
                    aria-label={resolvedLabel}
                    aria-checked={checked}
                    {...props}
                    id={id}
                />
                <Checkmark checked={checked} disabled={isDisabled} error={error} />
            </RadioBase>
            {resolvedLabel && (
                <RadioLabel>
                    <Label htmlFor={id}>
                        {resolvedLabel} {isRequired && <Required>*</Required>}
                    </Label>
                </RadioLabel>
            )}
        </RadioWrapper>
    );
};

export const RadioGroup = ({ isVertical, radios, name, ariaLabel }: RadioGroupProps) => {
    const [generatedName] = useState(() => `radio-group-${uuidv4()}`);
    const groupName = name || generatedName;

    if (!radios.length) {
        return <></>;
    }

    return (
        <RadioGroupContainer isVertical={isVertical} role="radiogroup" aria-label={ariaLabel}>
            {radios.map((checkbox) => {
                const props = { ...checkbox, name: groupName };
                return (
                    <React.Fragment key={checkbox.label}>
                        <Radio {...props} />
                    </React.Fragment>
                );
            })}
        </RadioGroupContainer>
    );
};
