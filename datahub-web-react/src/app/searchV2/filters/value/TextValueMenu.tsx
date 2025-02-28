import React from 'react';
import { Button, Typography } from 'antd';
import styled from 'styled-components';
import { FilterField, FilterValue } from '../types';
import TextValueInput from './TextValueInput';

const Container = styled.div`
    padding: 16px;
    background-color: #ffffff;
    box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
    border-radius: 8px;
`;

const Title = styled(Typography.Title)`
    padding-bottom: 4px;
`;

const UpdateButton = styled(Button)`
    margin-top: 12px;
`;

interface Props {
    field: FilterField;
    values: FilterValue[];
    onChangeValues: (newValues: FilterValue[]) => void;
    onApply: () => void;
}

export default function TextValueMenu({ field, values, onChangeValues, onApply }: Props) {
    const { displayName } = field;
    const value = values.length > 0 ? values[0].displayName || values[0].value : '';

    return (
        <Container>
            <Title level={5}>{`Filter by ${displayName}`}</Title>
            <TextValueInput
                name={displayName}
                value={value}
                onChangeValue={(v) => onChangeValues([{ value: v, entity: null }])}
            />
            <UpdateButton type="primary" onClick={onApply}>
                Apply
            </UpdateButton>
        </Container>
    );
}
