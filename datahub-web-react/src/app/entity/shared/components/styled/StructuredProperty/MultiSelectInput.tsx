import { Checkbox, Select, Tag } from 'antd';
import React from 'react';
import styled from 'styled-components';

import DropdownLabel from '@app/entity/shared/components/styled/StructuredProperty/DropdownLabel';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import ValueDescription from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/ValueDescription';
import { getStructuredPropertyValue } from '@app/entity/shared/utils';

import { AllowedValue } from '@types';

const StyledCheckbox = styled(Checkbox)<{ $displayBulkStyles?: boolean }>`
    display: flex;
    margin: 0 0 4px 0;
    .ant-checkbox-inner {
        border-color: ${ANTD_GRAY_V2[8]};
    }
    &&& {
        margin-left: 0;
    }
    ${(props) => props.$displayBulkStyles && 'color: white;'}
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
    const {
        prompt: { displayBulkPromptStyles },
    } = useEntityFormContext();

    const shouldShowSelectDropdown = allowedValues.length > 5 || displayBulkPromptStyles;

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
                    $displayBulkStyles={displayBulkPromptStyles}
                >
                    {getStructuredPropertyValue(allowedValue.value)}
                    {allowedValue.description && <ValueDescription description={allowedValue.description} />}
                </StyledCheckbox>
            ))}
        </div>
    );
}
