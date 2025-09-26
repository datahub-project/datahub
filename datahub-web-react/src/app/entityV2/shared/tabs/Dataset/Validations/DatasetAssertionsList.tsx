import { DeleteOutlined, DownOutlined, MoreOutlined, RightOutlined, StopOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Dropdown, Empty, Image, Modal, Tag, Typography, message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { StyledTable } from '@app/entityV2/shared/components/styled/StyledTable';
import AssertionMenu from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionMenu';
import { DatasetAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/DatasetAssertionDescription';
import { DatasetAssertionDetails } from '@app/entityV2/shared/tabs/Dataset/Validations/DatasetAssertionDetails';
import {
    getResultColor,
    getResultIcon,
    getResultText,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { useDeleteAssertionMutation } from '@graphql/assertion.generated';
import { Assertion, AssertionRunStatus } from '@types';

const ResultContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
`;

const ResultTypeText = styled(Typography.Text)`
    margin-left: 8px;
`;

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

const PlatformContainer = styled.div`
    margin-right: 8px;
`;

const StyledMoreOutlined = styled(MoreOutlined)`
    font-size: 18px;
`;

type Props = {
    assertions: Array<Assertion>;
    onDelete?: (urn: string) => void;
};

/**
 * A list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 *
 * Currently this component supports rendering Dataset Assertions only.
 */
export const DatasetAssertionsList = ({ assertions, onDelete }: Props) => {
    const [deleteAssertionMutation] = useDeleteAssertionMutation();

    const deleteAssertion = async (urn: string) => {
        try {
            await deleteAssertionMutation({
                variables: { urn },
            });
            message.success({ content: 'Removed assertion.', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove assertion: \n ${e.message || ''}`, duration: 3 });
            }
        }
        onDelete?.(urn);
    };

    const onDeleteAssertion = (urn: string) => {
        Modal.confirm({
            title: `Confirm Assertion Removal`,
            content: `Are you sure you want to remove this assertion from the dataset?`,
            onOk() {
                deleteAssertion(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const assertionsTableData = assertions.map((assertion) => ({
        urn: assertion.urn,
        type: assertion.info?.type,
        platform: assertion.platform,
        datasetAssertionInfo: assertion.info?.datasetAssertion,
        description: assertion.info?.description,
        lastExecTime: assertion.runEvents?.runEvents?.length && assertion.runEvents.runEvents[0].timestampMillis,
        lastExecResult:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0].result?.type,
    }));

    const assertionsTableCols = [
        {
            title: '',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                const executionDate = record.lastExecTime && new Date(record.lastExecTime);
                const localTime = executionDate && `${executionDate.toLocaleDateString()}`;
                const resultColor = (record.lastExecResult && getResultColor(record.lastExecResult)) || 'default';
                const resultText = (record.lastExecResult && getResultText(record.lastExecResult)) || 'No Evaluations';
                const resultIcon = (record.lastExecResult && getResultIcon(record.lastExecResult)) || <StopOutlined />;
                const { description } = record;
                return (
                    <ResultContainer>
                        <div>
                            <Tooltip title={(localTime && `Last evaluated on ${localTime}`) || 'No Evaluations'}>
                                <Tag style={{ borderColor: resultColor }}>
                                    {resultIcon}
                                    <ResultTypeText style={{ color: resultColor }}>{resultText}</ResultTypeText>
                                </Tag>
                            </Tooltip>
                        </div>
                        <DatasetAssertionDescription
                            description={description}
                            assertionInfo={record.datasetAssertionInfo}
                            lastEvaluation={record.lastEvaluation}
                        />
                    </ResultContainer>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: '',
            render: (_, record: any) => (
                <ActionButtonContainer>
                    <Tooltip
                        title={
                            record.platform.properties?.displayName || capitalizeFirstLetterOnly(record.platform.name)
                        }
                    >
                        <PlatformContainer>
                            {(record.platform.properties?.logoUrl && (
                                <Image
                                    preview={false}
                                    height={20}
                                    width={20}
                                    src={record.platform.properties?.logoUrl}
                                />
                            )) || (
                                <Typography.Text>
                                    {record.platform.properties?.displayName ||
                                        capitalizeFirstLetterOnly(record.platform.name)}
                                </Typography.Text>
                            )}
                        </PlatformContainer>
                    </Tooltip>
                    <Button onClick={() => onDeleteAssertion(record.urn)} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                    <Dropdown overlay={<AssertionMenu urn={record.urn} />} trigger={['click']}>
                        <StyledMoreOutlined />
                    </Dropdown>
                </ActionButtonContainer>
            ),
        },
    ];

    return (
        <>
            <StyledTable
                columns={assertionsTableCols}
                dataSource={assertionsTableData}
                rowKey="urn"
                locale={{
                    emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                expandable={{
                    defaultExpandAllRows: false,
                    expandRowByClick: true,
                    expandedRowRender: (record) => {
                        return <DatasetAssertionDetails urn={record.urn} lastEvaluatedAtMillis={record.lastExecTime} />;
                    },
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
