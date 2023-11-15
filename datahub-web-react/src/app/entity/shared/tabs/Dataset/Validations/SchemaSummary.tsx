import React from 'react';
import styled from 'styled-components';
import { MoreOutlined } from '@ant-design/icons';
import { Table } from 'antd';
import { SchemaMetadata } from '../../../../../../types.generated';
import { ANTD_GRAY } from '../../../constants';

const TitleText = styled.div`
    color: ${ANTD_GRAY[7]};
    margin-bottom: 20px;
    letter-spacing; 4px;

`;

const ColumnHeader = styled.div`
    color: ${ANTD_GRAY[8]};
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
    box-shadow: 0px 0px 4px rgba(0, 0, 0, 0.1);
    height: 100%;
`;

type Props = {
    schema: SchemaMetadata;
};

export const SchemaSummary = ({ schema }: Props) => {
    const columns = [
        {
            title: () => <ColumnHeader>NAME</ColumnHeader>,
            render: (field) => <>{field.fieldPath}</>,
        },
        {
            title: () => <ColumnHeader>TYPE</ColumnHeader>,
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
            <TitleText>SCHEMA</TitleText>
            <SummaryContainer>
                <StyledTable pagination={false} columns={columns} dataSource={data} />
            </SummaryContainer>
        </Container>
    );
};
