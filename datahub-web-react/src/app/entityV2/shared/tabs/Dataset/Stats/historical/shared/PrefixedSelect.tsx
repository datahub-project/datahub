import React from 'react';
import { Select, Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../../constants';

type Props = {
    prefixText: string;
    values: Array<string>;
    value: string;
    setValue: (value: string) => void;
};

const SubHeaderText = styled(Typography.Text)`
    font-size: 14px;
    color: ${ANTD_GRAY[8]};
`;

const EmbeddedSelect = styled(Select)`
    padding-left: 8px;
`;

export default function PrefixedSelect({ prefixText, values, value, setValue }: Props): JSX.Element {
    return (
        <span>
            <SubHeaderText>{prefixText}</SubHeaderText>
            <EmbeddedSelect value={value} onChange={(val) => setValue(val as string)}>
                {values.map((val) => (
                    <Select.Option value={val}>{val}</Select.Option>
                ))}
            </EmbeddedSelect>
        </span>
    );
}
