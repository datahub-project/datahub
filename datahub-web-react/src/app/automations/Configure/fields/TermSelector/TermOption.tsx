import React, { useRef, useEffect, useState } from 'react';

import { Radio } from 'antd';
import styled from 'styled-components';

import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { sharedStyles } from '@app/automations/sharedComponents';

import type { SelectDropdownProps, RadioValue } from './types';
import { SelectDropdown } from './SelectDropdown';

const Wrapper = styled.div`
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 28px;
    border: 1px solid ${sharedStyles.borderColor};
    border-radius: ${sharedStyles.borderRadius};
    padding: 16px;
`;

const DropdownsWrapper = styled.div`
    display: grid;
    gap: 16px;
    width: 100%;
`;

const ContentWrapper = styled.div`
    display: flex;
    background-color: rgba(0, 0, 0, 0.03);
    border-radius: ${sharedStyles.borderRadius};
    padding: 16px;
    align-items: center;
    justify-content: center;
`;

const StyledRadioGroup = styled(Radio.Group)`
    display: flex;
    flex-direction: column;
    gap: 0;

    .ant-radio-wrapper {
        margin-bottom: 2px;
        font-weight: 400 !important;
    }
`;

export const Label = styled.div`
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 4px;

    &.heading {
        font-size: 14px;
    }
`;

type Props = {
    shortType: string;
    selects: SelectDropdownProps[];
    radio: {
        preselectedValue: RadioValue;
    };
    onChange: (value: any) => void;
};

export const TermOption = ({ shortType, selects, radio, onChange }: Props) => {
    const { preselectedValue } = radio;

    const [selectedOptions, setSelectedOptions] = useState<any>({
        [shortType.toLowerCase()]: [],
    });
    const [radioValue, setRadioValue] = useState<RadioValue>(preselectedValue);

    // Handle the change of the selected terms
    const handleTermsChange = (values: any) => {
        setSelectedOptions({
            ...selectedOptions,
            ...values,
        });
    };

    const prevProps = useRef(selectedOptions);

    // Send the data back to the parent component
    // Only sends the data if the form data has changed
    useEffect(() => {
        const prevData = prevProps.current;
        const hasChanged = JSON.stringify(prevData) !== JSON.stringify({ selectedOptions, selectedType: radioValue });
        if (hasChanged) {
            onChange({
                selectionType: radioValue,
                selected: selectedOptions,
            });
        }
        prevProps.current = { selectedOptions, selectedType: radioValue };
    }, [selectedOptions, radioValue, onChange]);

    return (
        <Wrapper>
            {/* Radio for selection type */}
            <div>
                <Label>Allowed {shortType}</Label>
                <StyledRadioGroup
                    options={[
                        { label: `${capitalizeFirstLetterOnly(shortType)} in a specific set`, value: 'some' },
                        { label: `All ${shortType}`, value: 'all' },
                        { label: 'None', value: 'none' },
                    ]}
                    value={radioValue}
                    onChange={(e) => setRadioValue(e.target.value)}
                />
            </div>
            {/* Select dropdown for selecting options */}
            <ContentWrapper>
                {radioValue === 'all' && (
                    <Label className="heading">All {shortType.toLowerCase()} will be propagated.</Label>
                )}
                {radioValue === 'some' && (
                    <DropdownsWrapper>
                        {/* Map of dropdowns based on the props */}
                        {selects.map((select) => (
                            <SelectDropdown
                                key={select.type}
                                placeholder={`Select ${select.label}…`}
                                onChange={(value) => handleTermsChange(value)}
                                {...select}
                            />
                        ))}
                    </DropdownsWrapper>
                )}
                {radioValue === 'none' && (
                    <Label className="heading">No {shortType.toLowerCase()} will be propagated.</Label>
                )}
            </ContentWrapper>
        </Wrapper>
    );
};
