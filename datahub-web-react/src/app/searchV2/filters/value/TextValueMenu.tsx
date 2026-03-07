import React from 'react';
import styled from 'styled-components';

import { FilterField, FilterValue } from '@app/searchV2/filters/types';
import TextValueInput from '@app/searchV2/filters/value/TextValueInput';

const Container = styled.div`
    padding: 16px;
    background-color: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    border-radius: 12px;
`;

const Title = styled.div`
    font-size: 16px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    padding-bottom: 4px;
`;

interface Props {
    field: FilterField;
    values: FilterValue[];
    onChangeValues: (newValues: FilterValue[]) => void;
}

export default function TextValueMenu({ field, values, onChangeValues }: Props) {
    const { displayName } = field;
    const value = values.length > 0 ? values[0].displayName || values[0].value : '';

    return (
        <Container>
            <Title>{`Filter by ${displayName}`}</Title>
            <TextValueInput
                name={displayName}
                value={value}
                onChangeValue={(v) => onChangeValues([{ value: v, entity: null }])}
            />
        </Container>
    );
}
