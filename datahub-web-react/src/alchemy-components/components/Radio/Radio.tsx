import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

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

    useEffect(() => {
        setChecked(isChecked || false);
    }, [isChecked]);

    const id = props.id || `checkbox-${resolvedLabel}`;

    return (
        <RadioWrapper disabled={isDisabled} error={error}>
            <RadioBase>
                <HiddenInput
                    type="radio"
                    id={resolvedLabel}
                    value={resolvedLabel}
                    checked={checked}
                    disabled={isDisabled}
                    onChange={() => {
                        setChecked(true);
                        setIsChecked?.(true);
                    }}
                    aria-label={resolvedLabel}
                    aria-labelledby={id}
                    aria-checked={checked}
                    {...props}
                />
                <Checkmark checked={checked} disabled={isDisabled} error={error} />
            </RadioBase>
            {resolvedLabel && (
                <RadioLabel>
                    <Label onClick={() => setChecked(true)}>
                        {resolvedLabel} {isRequired && <Required>*</Required>}
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
