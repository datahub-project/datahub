import { Button, Empty, Image, Tag, Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import {
    CheckCircleOutlined,
    CloseCircleOutlined,
    DeleteOutlined,
    DownOutlined,
    RightOutlined,
} from '@ant-design/icons';
import { useGetDatasetAssertionsQuery } from '../../../../../../graphql/dataset.generated';
import { DatasetAssertionDescription } from './descriptions/DatasetAssertionDescription';
import { StyledTable } from '../../../components/styled/StyledTable';
import { AssertionHistory } from './AssertionHistory';
import { Assertion, AssertionResultType, AssertionRunStatus } from '../../../../../../types.generated';

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

const PlatformContainer = styled.div`
    margin-right: 8px;
`;

export const AssertionsList = ({ urn }: { urn: string }) => {
    const { data } = useGetDatasetAssertionsQuery({ variables: { urn } });

    const assertions =
        (data && data.dataset?.assertions?.relationships?.map((relationship) => relationship.entity as Assertion)) ||
        [];

    const tableColumns = [
        {
            title: 'Last Result',
            dataIndex: 'lastExecResult',
            key: 'lastExecResult',
            render: (lastExecResult: string, record: any) => {
                const executionDate = record.lastExecTime && new Date(record.lastExecTime);
                const localTime = executionDate && `${executionDate.toUTCString()}`;
                const resultColor = lastExecResult === AssertionResultType.Success ? 'green' : 'red';
                const assertionEntityType = record.type;
                if (assertionEntityType !== 'DATASET') {
                    throw new Error(`Unsupported Assertion Type ${assertionEntityType} found.`);
                }
                return (
                    <>
                        {lastExecResult === AssertionResultType.Success ? (
                            <Tooltip title={`Last run at ${localTime}`}>
                                <Tag color={resultColor}>
                                    <CheckCircleOutlined style={{ color: resultColor }} />
                                    <Typography.Text style={{ marginLeft: 8, color: resultColor }}>
                                        Passed
                                    </Typography.Text>
                                </Tag>
                            </Tooltip>
                        ) : (
                            <CloseCircleOutlined />
                        )}
                        <DatasetAssertionDescription
                            assertionInfo={record.datasetAssertionInfo}
                            parameters={record.parameters}
                        />
                    </>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <ActionButtonContainer>
                    <PlatformContainer>
                        {(record.platform.properties?.logoUrl && (
                            <Image height={20} width={20} src={record.platform.properties?.logoUrl} />
                        )) || <Typography.Text>{record.platform.properties?.displayName}</Typography.Text>}
                    </PlatformContainer>
                    <Button onClick={() => null} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                </ActionButtonContainer>
            ),
        },
    ];

    const tableData = assertions.map((assertion) => ({
        urn: assertion.urn,
        type: assertion.info?.type,
        platform: assertion.platform,
        lastExecTime: assertion.runEvents?.length && assertion.runEvents[0].timestampMillis,
        lastExecResult:
            assertion.runEvents?.length &&
            assertion.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents[0].result?.type,
        datasetAssertionInfo: assertion.info?.datasetAssertion,
    }));

    return (
        <>
            <StyledTable
                columns={tableColumns}
                dataSource={tableData}
                rowKey="urn"
                locale={{
                    emptyText: <Empty description="No Assertions Found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                expandable={{
                    expandedRowRender: (record) => {
                        return <AssertionHistory urn={record.urn} />;
                    },
                    defaultExpandAllRows: false,
                    indentSize: 0,
                    expandIcon: ({ expanded, onExpand, record }: any) =>
                        expanded ? (
                            <DownOutlined style={{ fontSize: 8 }} onClick={(e) => onExpand(record, e)} />
                        ) : (
                            <RightOutlined style={{ fontSize: 8 }} onClick={(e) => onExpand(record, e)} />
                        ),
                }}
                showHeader={false}
                pagination={false}
            />
        </>
    );
};
