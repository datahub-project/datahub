import { MoreOutlined } from '@ant-design/icons';
import { Table } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SchemaMetadata } from '@types';

const TitleText = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
    margin-bottom: 20px;
    letter-spacing; 4px;

`;

const ColumnHeader = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    letter-spacing; 4px;
`;

const Container = styled.div`
    padding: 28px;
    height: 100%;
`;

const SummaryContainer = styled.div`
    width: 100%;
    display: flex;
    align-items: center;
`;

const StyledTable = styled(Table)`
    width: 100%;
    border-radius: 8px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    height: 100%;
`;

type Props = {
    schema: SchemaMetadata;
};

export const SchemaSummary = ({ schema }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const columns = [
        {
            title: () => <ColumnHeader>{t('schemaSummary.nameColumn')}</ColumnHeader>,
            render: (field) => <>{field.fieldPath}</>,
        },
        {
            title: () => <ColumnHeader>{t('schemaSummary.typeColumn')}</ColumnHeader>,
            render: (field) => <>{field.nativeDataType}</>,
        },
        {
            title: () => <MoreOutlined />,
            render: (_) => undefined,
        },
    ];

    const data = (schema?.fields || []).map((field) => ({
        ...field,
        key: field.fieldPath,
    }));

    return (
        <Container>
            <TitleText>{t('schemaSummary.schemaTitle')}</TitleText>
            <SummaryContainer>
                <StyledTable pagination={false} columns={columns} dataSource={data} />
            </SummaryContainer>
        </Container>
    );
};
