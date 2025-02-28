import React from 'react';
import styled from 'styled-components';
import { Table } from 'antd';
import { SchemaContract } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import { DataContractSummaryFooter } from './DataContractSummaryFooter';

const TitleText = styled.div`
    color: ${ANTD_GRAY[7]};
    margin-bottom: 20px;
    letter-spacing: 1px;
`;

const ColumnHeader = styled.div`
    color: ${ANTD_GRAY[8]};
`;

const Container = styled.div`
    padding: 28px;
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
    contracts: SchemaContract[];
    showAction?: boolean;
};

export const SchemaContractSummary = ({ contracts, showAction = false }: Props) => {
    const firstContract = (contracts.length && contracts[0]) || undefined;
    const schemaMetadata = firstContract?.assertion?.info?.schemaAssertion?.schema;

    const columns = [
        {
            title: () => <ColumnHeader>NAME</ColumnHeader>,
            render: (field) => <>{field.fieldPath}</>,
        },
        {
            title: () => <ColumnHeader>TYPE</ColumnHeader>,
            render: (field) => <>{field.type}</>,
        },
    ];

    const data = (schemaMetadata?.fields || []).map((field) => ({
        ...field,
        key: field.fieldPath,
    }));

    return (
        <Container>
            <TitleText>SCHEMA</TitleText>
            <SummaryContainer>
                <StyledTable
                    pagination={false}
                    columns={columns}
                    dataSource={data}
                    footer={() => (
                        <DataContractSummaryFooter
                            assertions={(firstContract && [firstContract?.assertion]) || []}
                            passingText="Meeting schema contract"
                            failingText="Violating schema contract"
                            errorText="Schema contract assertions are completing with errors"
                            actionText="view schema assertions"
                            showAction={showAction}
                        />
                    )}
                />
            </SummaryContainer>
        </Container>
    );
};
