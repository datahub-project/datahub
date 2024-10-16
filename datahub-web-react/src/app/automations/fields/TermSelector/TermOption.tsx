import React, { useState, useEffect } from 'react';
import { Radio } from 'antd';
import styled from 'styled-components';

import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { sharedStyles } from '@app/automations/sharedComponents';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import GlossaryTermsSelector from '@src/app/govern/Dashboard/Forms/questionTypes/GlossaryTermsSelector';

import { EntityType } from '@src/types.generated';
import type { SelectDropdownProps, RadioValue } from './types';
import { SelectDropdown } from './SelectDropdown';

const Wrapper = styled.div`
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 28px;
    border: 1px solid ${sharedStyles.borderColor};
    border-radius: ${sharedStyles.borderRadius};
    padding: 16px;

    &.termOptionSingleContainer {
        gap: 0px;
        padding: 0px;
        width: 100%;
        display: block;
        label {
            display: flex !important;
        }
    }
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

    &.termSelector {
        padding: 0px;
        border: none;
    }
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
    type: EntityType;
    typeName: string;
    selects: SelectDropdownProps[];
    radio: {
        allowedRadios: string[];
        preselectedValue: RadioValue;
    };
    onChange: (value: any, entity: EntityType) => void;
};

type SelectedOptionType = {
    GLOSSARY_NODE: string[];
    GLOSSARY_TERM: string[];
};

const INITIAL_SELECTED_VALUE = {
    GLOSSARY_TERM: [],
    GLOSSARY_NODE: [],
};

export const TermOption = ({ type, typeName, selects, radio, onChange }: Props) => {
    const { allowedRadios, preselectedValue } = radio;

    // Internal state for selected options
    const [selectedOptions, setSelectedOptions] = useState<SelectedOptionType>(() => {
        const initialSelected = {};
        selects.forEach((select) => {
            initialSelected[select.type] = select.preselectedOptions || [];
        });
        return initialSelected as SelectedOptionType;
    });

    // Internal state for radio value
    const [radioValue, setRadioValue] = useState<RadioValue>(preselectedValue);

    // Handle the change of the selected terms
    const handleTermsChange = (values: any, entity: EntityType) => {
        const newSelectedOptions = {
            ...selectedOptions,
            ...values,
        };
        setSelectedOptions(newSelectedOptions);
        onChange(
            {
                selectionType: radioValue,
                selected: newSelectedOptions,
            },
            entity,
        );
    };

    // Handle the change of the radio value
    const handleRadioChange = (value: RadioValue) => {
        setRadioValue(value);
        onChange(
            {
                selectionType: value,
                selected: selectedOptions,
            },
            type,
        );
    };

    // Update the radio value when the preselectedValue prop changes
    useEffect(() => {
        setRadioValue(preselectedValue);
    }, [preselectedValue]);

    // Set the selected options based on the preselected options
    useEffect(() => {
        const updatedSelectedOptions = INITIAL_SELECTED_VALUE;
        selects.forEach((select) => {
            updatedSelectedOptions[select.type] = select.preselectedOptions || [];
        });
        setSelectedOptions(updatedSelectedOptions);
    }, [selects]);

    // Configurable options
    const options: { label: string; value: string }[] = [];
    if (allowedRadios.includes('all')) {
        options.push({ label: `All ${typeName}`, value: 'all' });
    }
    if (allowedRadios.includes('some')) {
        options.push({ label: `${capitalizeFirstLetterOnly(typeName)} in a specific set`, value: 'some' });
    }
    if (allowedRadios.includes('none')) {
        options.push({ label: 'None', value: 'none' });
    }
    const onUpdate = (values: SelectOption[]) => {
        const newSelectedOptions: SelectedOptionType = {
            GLOSSARY_NODE: [],
            GLOSSARY_TERM: [],
        };

        values?.forEach((glossary) => {
            const node = (glossary?.isParent && glossary?.value) || '';
            const term = (!glossary?.isParent && glossary?.value) || '';
            if (node && !newSelectedOptions.GLOSSARY_NODE.includes(node)) {
                newSelectedOptions.GLOSSARY_NODE.push(node);
            }
            if (term && !newSelectedOptions.GLOSSARY_TERM.includes(term)) {
                newSelectedOptions.GLOSSARY_TERM.push(term);
            }
        });
        setSelectedOptions(newSelectedOptions);
        onChange(
            {
                selectionType: radioValue,
                selected: newSelectedOptions,
            },
            type,
        );
    };

    const initialOptions = selectedOptions?.GLOSSARY_TERM?.map((urn) => ({ value: urn })) || [];

    return (
        <Wrapper
            className={
                options.length === 1 && type === EntityType.GlossaryTerm ? 'termOptionSingleContainer' : undefined
            }
        >
            {/* Radio for selection type */}
            {options.length > 1 && (
                <div>
                    <Label>Allowed {typeName}</Label>
                    <StyledRadioGroup
                        options={options}
                        value={radioValue}
                        onChange={(e) => handleRadioChange(e.target.value)}
                    />
                </div>
            )}

            {/* Select dropdown for selecting options */}
            <ContentWrapper
                className={options.length === 1 && type === EntityType.GlossaryTerm ? 'termSelector' : undefined}
            >
                {radioValue === 'all' && (
                    <Label className="heading">All {typeName.toLowerCase()} will be propagated.</Label>
                )}
                {radioValue === 'some' && (
                    <DropdownsWrapper>
                        {type === EntityType.GlossaryTerm && options.length === 1 ? (
                            <GlossaryTermsSelector
                                onUpdate={onUpdate}
                                initialOptions={initialOptions}
                                areNodeSelectable
                            />
                        ) : (
                            selects.map((select) => (
                                <SelectDropdown
                                    key={select.type}
                                    placeholder={`Select ${select.label}…`}
                                    onChange={(value) => handleTermsChange(value, select.type)}
                                    {...select}
                                />
                            ))
                        )}
                    </DropdownsWrapper>
                )}
                {radioValue === 'none' && (
                    <Label className="heading">No {typeName.toLowerCase()} will be propagated.</Label>
                )}
            </ContentWrapper>
        </Wrapper>
    );
};
