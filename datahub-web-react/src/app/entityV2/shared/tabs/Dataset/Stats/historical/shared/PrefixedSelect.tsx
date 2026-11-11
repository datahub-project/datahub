import { SimpleSelect, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

type Props = {
    prefixText: string;
    values: Array<string>;
    value: string;
    setValue: (value: string) => void;
};

const Container = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 8px;
`;

const SubHeaderText = styled(Text).attrs({ type: 'span' })`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
    line-height: inherit;
    word-break: break-word;
`;

export default function PrefixedSelect({ prefixText, values, value, setValue }: Props): JSX.Element {
    return (
        <Container>
            <SubHeaderText>{prefixText}</SubHeaderText>
            <SimpleSelect
                options={values.map((val) => ({ value: val, label: val }))}
                values={[value]}
                onUpdate={(selectedValues) => setValue(selectedValues[0])}
                showClear={false}
                width="fit-content"
            />
        </Container>
    );
}
