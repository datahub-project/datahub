import { CopyOutlined, StopOutlined } from '@ant-design/icons';
import { Button, Divider, Empty, Tag, Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import React from 'react';
import { TestResult } from '../../../../../../types.generated';
import { StyledTable } from '../../../components/styled/StyledTable';
import { getResultColor, getResultIcon, getResultText } from './testUtils';

const ResultContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
`;

const TestResultsContainer = styled.div`
    padding: 20px;
`;

const TestName = styled(Typography.Title)`
    margin: 0px;
    padding: 0px;

    && {
        margin-bottom: 4px;
    }
`;

const TestCategory = styled(Typography.Text)`
    margin: 0px;
    padding: 0px;
`;

const ResultTypeText = styled(Typography.Text)`
    margin-left: 8px;
`;

type Props = {
    title: string;
    results: Array<TestResult>;
};

export const TestResultsList = ({ title, results }: Props) => {
    const resultsTableData = results.map((result) => ({
        urn: result.test?.urn,
        name: result?.test?.name,
        category: result?.test?.category,
        description: result?.test?.description,
        resultType: result.type,
    }));

    const resultsTableCols = [
        {
            title: '',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                const resultColor = (record.resultType && getResultColor(record.resultType)) || 'default';
                const resultText = (record.resultType && getResultText(record.resultType)) || 'No Evaluations';
                const resultIcon = (record.resultType && getResultIcon(record.resultType)) || <StopOutlined />;
                return (
                    <ResultContainer>
                        <div>
                            <Tag style={{ borderColor: resultColor }}>
                                {resultIcon}
                                <ResultTypeText style={{ color: resultColor }}>{resultText}</ResultTypeText>
                            </Tag>
                        </div>
                        <div style={{ width: '100%', display: 'flex', justifyContent: 'space-between' }}>
                            <div style={{ marginLeft: 8 }}>
                                <div>
                                    <TestName level={5}>{record.name}</TestName>
                                    <TestCategory type="secondary">{record.category}</TestCategory>
                                    <Divider type="vertical" />
                                    <Typography.Text type={record.description ? undefined : 'secondary'}>
                                        {record.description || 'No description'}
                                    </Typography.Text>
                                </div>
                            </div>
                            {navigator.clipboard && (
                                <Tooltip title="Copy URN. An URN uniquely identifies an entity on DataHub.">
                                    <Button
                                        icon={<CopyOutlined />}
                                        onClick={() => {
                                            navigator.clipboard.writeText(record.urn);
                                        }}
                                    />
                                </Tooltip>
                            )}
                        </div>
                    </ResultContainer>
                );
            },
        },
    ];

    return (
        <TestResultsContainer>
            <Typography.Title level={5}>{title}</Typography.Title>
            <StyledTable
                columns={resultsTableCols}
                dataSource={resultsTableData}
                rowKey="urn"
                locale={{
                    emptyText: <Empty description="No Tests Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                showHeader={false}
                pagination={false}
            />
        </TestResultsContainer>
    );
};
