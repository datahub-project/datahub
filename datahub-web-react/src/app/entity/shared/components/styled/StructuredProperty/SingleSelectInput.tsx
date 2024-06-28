import { Radio, Select } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY_V2 } from '../../../constants';
import { getStructuredPropertyValue } from '../../../utils';
import ValueDescription from '../../../entityForm/prompts/StructuredPropertyPrompt/ValueDescription';
import { AllowedValue } from '../../../../../../types.generated';
import DropdownLabel from './DropdownLabel';

const StyledRadio = styled(Radio)`
    display: block;
    .ant-radio-inner {
        border-color: ${ANTD_GRAY_V2[8]};
    }
`;

const DROPDOWN_STYLE = { minWidth: 320, maxWidth: 320, textAlign: 'left', fontSize: '14px' };

interface Props {
    selectedValues: any[];
    allowedValues: AllowedValue[];
    selectSingleValue: (value: string | number) => void;
}

export default function SingleSelectInput({ selectSingleValue, allowedValues, selectedValues }: Props) {
    return allowedValues.length > 5 ? (
        <Select
            style={DROPDOWN_STYLE as any}
            placeholder="Select"
            value={selectedValues}
            onSelect={(value) => selectSingleValue(value)}
            optionLabelProp="value"
        >
            {allowedValues.map((allowedValue) => (
                <Select.Option value={getStructuredPropertyValue(allowedValue.value)}>
                    <DropdownLabel
                        value={getStructuredPropertyValue(allowedValue.value)}
                        description={allowedValue.description}
                    />
                </Select.Option>
            ))}
        </Select>
    ) : (
        <Radio.Group value={selectedValues[0]} onChange={(e) => selectSingleValue(e.target.value)}>
            {allowedValues.map((allowedValue) => (
                <StyledRadio
                    key={getStructuredPropertyValue(allowedValue.value)}
                    value={getStructuredPropertyValue(allowedValue.value)}
                >
                    {getStructuredPropertyValue(allowedValue.value)}
                    {allowedValue.description && <ValueDescription description={allowedValue.description} />}
                </StyledRadio>
            ))}
        </Radio.Group>
    );
}
