import { Button, Empty, Image, Tag, Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { DeleteOutlined, DownOutlined, RightOutlined, StopOutlined } from '@ant-design/icons';
import { DatasetAssertionDescription } from './descriptions/DatasetAssertionDescription';
import { StyledTable } from '../../../components/styled/StyledTable';
import { AssertionDetails } from './AssertionDetails';
import { Assertion, AssertionResultType, AssertionRunStatus } from '../../../../../../types.generated';
import { getResultColor, getResultIcon, getResultText } from './assertionUtils';

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

const PlatformContainer = styled.div`
    margin-right: 8px;
`;

export const AssertionsList = ({ assertions }: { assertions: Array<Assertion> }) => {
    const tableColumns = [
        {
            title: 'Last Result',
            dataIndex: 'lastExecResult',
            key: 'lastExecResult',
            render: (lastExecResult: AssertionResultType, record: any) => {
                const executionDate = record.lastExecTime && new Date(record.lastExecTime);
                const localTime = executionDate && `${executionDate.toLocaleDateString()}`;
                const resultColor = (lastExecResult && getResultColor(lastExecResult)) || 'default';
                const resultText = (lastExecResult && getResultText(lastExecResult)) || 'No Evaluations';
                const resultIcon = (lastExecResult && getResultIcon(lastExecResult)) || <StopOutlined />;
                const assertionEntityType = record.type;
                if (assertionEntityType !== 'DATASET') {
                    throw new Error(`Unsupported Assertion Type ${assertionEntityType} found.`);
                }
                return (
                    <span
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'left',
                        }}
                    >
                        <div>
                            <Tooltip title={(localTime && `Last evaluated on ${localTime}`) || 'No Evaluations'}>
                                <Tag color={resultColor}>
                                    {resultIcon}
                                    <Typography.Text style={{ marginLeft: 8, color: resultColor }}>
                                        {resultText}
                                    </Typography.Text>
                                </Tag>
                            </Tooltip>
                        </div>
                        <DatasetAssertionDescription
                            assertionInfo={record.datasetAssertionInfo}
                            parameters={record.parameters}
                            logic={record.logic}
                        />
                    </span>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (_, record: any) => (
                <ActionButtonContainer>
                    <Tooltip title={record.platform.properties?.displayName}>
                        <PlatformContainer>
                            {(record.platform.properties?.logoUrl && (
                                <Image
                                    preview={false}
                                    height={20}
                                    width={20}
                                    src={record.platform.properties?.logoUrl}
                                />
                            )) || <Typography.Text>{record.platform.properties?.displayName}</Typography.Text>}
                        </PlatformContainer>
                    </Tooltip>
                    <Button hidden onClick={() => null} type="text" shape="circle" danger>
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
        parameters: assertion.info?.parameters,
        logic: assertion.info?.logic,
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
                        return <AssertionDetails urn={record.urn} lastReportedAt={record.lastExecTime} />;
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
