/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Radio, Select } from 'antd';
import React from 'react';
import styled from 'styled-components';

import DropdownLabel from '@app/entity/shared/components/styled/StructuredProperty/DropdownLabel';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import ValueDescription from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/ValueDescription';
import { getStructuredPropertyValue } from '@app/entity/shared/utils';

import { AllowedValue } from '@types';

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
