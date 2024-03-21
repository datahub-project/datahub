import { Checkbox, Select, Tag } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../constants';
import { getStructuredPropertyValue } from '../../../utils';
import ValueDescription from '../../../entityForm/prompts/StructuredPropertyPrompt/ValueDescription';
import { AllowedValue } from '../../../../../../types.generated';
import DropdownLabel from './DropdownLabel';

const StyledCheckbox = styled(Checkbox)`
    display: flex;
    margin: 0 0 4px 0;
    .ant-checkbox-inner {
        border-color: ${ANTD_GRAY_V2[8]};
    }
    &&& {
        margin-left: 0;
    }
`;

const StyleTag = styled(Tag)`
    font-family: Manrope;
    font-size: 14px;
    font-style: normal;
    font-weight: 400;
`;

const DROPDOWN_STYLE = { minWidth: 320, maxWidth: 320, textAlign: 'left' };

interface Props {
    selectedValues: any[];
    allowedValues: AllowedValue[];
    toggleSelectedValue: (value: string | number) => void;
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function MultiSelectInput({
    toggleSelectedValue,
    updateSelectedValues,
    allowedValues,
    selectedValues,
}: Props) {
    const shouldShowSelectDropdown = allowedValues.length > 5;

    return shouldShowSelectDropdown ? (
        <Select
            style={DROPDOWN_STYLE as any}
            placeholder="Select"
            value={selectedValues}
            mode="multiple"
            options={allowedValues.map((allowedValue) => ({
                value: getStructuredPropertyValue(allowedValue.value),
                label: (
                    <DropdownLabel
                        value={getStructuredPropertyValue(allowedValue.value)}
                        description={allowedValue.description}
                    />
                ),
            }))}
            tagRender={(tagProps: any) => {
                return (
                    <StyleTag closable={tagProps.closable} onClose={tagProps.onClose}>
                        {tagProps.value}
                    </StyleTag>
                );
            }}
            onChange={(value) => updateSelectedValues(value)}
        />
    ) : (
        <div>
            {allowedValues.map((allowedValue) => (
                <StyledCheckbox
                    key={getStructuredPropertyValue(allowedValue.value)}
                    value={getStructuredPropertyValue(allowedValue.value)}
                    onChange={(e) => toggleSelectedValue(e.target.value)}
                    checked={selectedValues.includes(getStructuredPropertyValue(allowedValue.value))}
                >
                    {getStructuredPropertyValue(allowedValue.value)}
                    {allowedValue.description && <ValueDescription description={allowedValue.description} />}
                </StyledCheckbox>
            ))}
        </div>
    );
}
