import { Button, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { FilterField, FilterValue } from '@app/searchV2/filters/types';
import TextValueInput from '@app/searchV2/filters/value/TextValueInput';

const Container = styled.div`
    padding: 16px;
    background-color: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowMd};
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
    const { t } = useTranslation('search');
    const { t: tc } = useTranslation('common.actions');
    const { displayName } = field;
    const value = values.length > 0 ? values[0].displayName || values[0].value : '';

    return (
        <Container>
            <Title level={5}>{t('filters.filterBy', { name: displayName })}</Title>
            <TextValueInput
                name={displayName}
                value={value}
                onChangeValue={(v) => onChangeValues([{ value: v, entity: null }])}
            />
            <UpdateButton type="primary" onClick={onApply}>
                {tc('apply')}
            </UpdateButton>
        </Container>
    );
}
