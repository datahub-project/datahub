import React from 'react';
import styled from 'styled-components';
import { Table } from 'antd';
import { Assertion, DataQualityContract, DatasetAssertionInfo } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import { DataContractAssertionStatus } from './DataContractAssertionStatus';
import { DataContractSummaryFooter } from './DataContractSummaryFooter';
import { DatasetAssertionDescription } from '../DatasetAssertionDescription';
import { FieldAssertionDescription } from '../FieldAssertionDescription';
import { SqlAssertionDescription } from '../SqlAssertionDescription';
import { VolumeAssertionDescription } from '../VolumeAssertionDescription';

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
`;

type Props = {
    contracts: DataQualityContract[];
    showAction?: boolean;
};

export const DataQualityContractSummary = ({ contracts, showAction = false }: Props) => {
    const assertions: Assertion[] = contracts?.map((contract) => contract.assertion);

    const columns = [
        {
            title: () => <ColumnHeader>ASSERTION</ColumnHeader>,
            render: (assertion: Assertion) => (
                <>
                    {assertion.info?.datasetAssertion && (
                        <DatasetAssertionDescription
                            assertionInfo={assertion.info?.datasetAssertion as DatasetAssertionInfo}
                        />
                    )}
                    {assertion.info?.volumeAssertion && (
                        <VolumeAssertionDescription assertionInfo={assertion.info?.volumeAssertion} />
                    )}
                    {assertion.info?.fieldAssertion && (
                        <FieldAssertionDescription assertionInfo={assertion.info?.fieldAssertion} />
                    )}
                    {assertion.info?.sqlAssertion && <SqlAssertionDescription assertionInfo={assertion.info} />}
                </>
            ),
        },
        {
            title: () => <ColumnHeader style={{ display: 'flex', justifyContent: 'center' }}>STATUS</ColumnHeader>,
            render: (assertion: Assertion) => <DataContractAssertionStatus assertion={assertion} />,
        },
    ];

    const data = (assertions || []).map((assertion) => ({
        ...assertion,
        key: assertion.urn,
    }));

    return (
        <Container>
            <TitleText>DATA QUALITY</TitleText>
            <SummaryContainer>
                <StyledTable
                    pagination={false}
                    columns={columns}
                    dataSource={data}
                    footer={() => (
                        <DataContractSummaryFooter
                            assertions={assertions}
                            passingText="Meeting data quality contract"
                            failingText="Violating data quality contract"
                            errorText="Data quality contract assertions are completing with errors"
                            actionText="view data quality assertions"
                            showAction={showAction}
                        />
                    )}
                />
            </SummaryContainer>
        </Container>
    );
};
