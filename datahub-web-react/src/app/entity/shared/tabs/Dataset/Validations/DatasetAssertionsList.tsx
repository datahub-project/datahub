import { Empty, Image, message, Modal, Tag, Tooltip, Typography, Dropdown, Menu } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import {
    DeleteOutlined,
    DownOutlined,
    MoreOutlined,
    RightOutlined,
    SettingOutlined,
    StopOutlined,
} from '@ant-design/icons';
import { DatasetAssertionDescription } from './DatasetAssertionDescription';
import { StyledTable } from '../../../components/styled/StyledTable';
import { DatasetAssertionDetails } from './DatasetAssertionDetails';
import { Assertion, AssertionRunStatus, AssertionType } from '../../../../../../types.generated';
import { getResultColor, getResultIcon, getResultText } from './assertionUtils';
import { useDeleteAssertionMutation } from '../../../../../../graphql/assertion.generated';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import { FreshnessAssertionDescription } from './FreshnessAssertionDescription';
import { LinkWrapper } from '../../../../../shared/LinkWrapper';
import { AssertionActionsBuilderModal } from './assertion/builder/AssertionActionsBuilderModal';

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

type Props = {
    assertions: Array<Assertion>;
    onDelete?: (urn: string) => void;
    onUpdate?: (assertion: Assertion) => void;
};

const UNKNOWN_DATA_PLATFORM = 'urn:li:dataPlatform:unknown';

/**
 * A list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 *
 * Currently this component supports rendering Dataset Assertions only.
 */
export const DatasetAssertionsList = ({ assertions, onDelete, onUpdate }: Props) => {
    const [deleteAssertionMutation] = useDeleteAssertionMutation();
    const [managingAssertion, setManagingAssertion] = useState<Assertion | undefined>(undefined);

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

    const onManageAssertion = (assertion: Assertion) => {
        // Only allow editing of the actions for now.
        // todo: allow updating the entire assertion definition. For now, we only support changing action configs (final step).
        setManagingAssertion(assertion);
    };

    const onUpdateAssertion = (assertion: Assertion) => {
        onUpdate?.(assertion);
        setManagingAssertion(undefined);
    };

    const assertionsTableData = assertions.map((assertion) => ({
        urn: assertion.urn,
        type: assertion.info?.type,
        platform: assertion.platform,
        datasetAssertionInfo: assertion.info?.datasetAssertion,
        freshnessAssertionInfo: assertion.info?.freshnessAssertion,
        lastExecTime: assertion.runEvents?.runEvents?.length && assertion.runEvents.runEvents[0].timestampMillis,
        lastExecResult:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0].result?.type,
        lastExecUrl:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0].result?.externalUrl,
        assertion,
    }));

    const assertionsTableCols = [
        {
            title: '',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                const assertionType = record.type;
                const executionDate = record.lastExecTime && new Date(record.lastExecTime);
                const localTime = executionDate && `${executionDate.toLocaleDateString()}`;
                const resultColor = (record.lastExecResult && getResultColor(record.lastExecResult)) || 'default';
                const resultText = (record.lastExecResult && getResultText(record.lastExecResult)) || 'No Evaluations';
                const resultIcon = (record.lastExecResult && getResultIcon(record.lastExecResult)) || <StopOutlined />;
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
                        {(assertionType === AssertionType.Dataset && (
                            <DatasetAssertionDescription assertionInfo={record.datasetAssertionInfo} />
                        )) || <FreshnessAssertionDescription assertionInfo={record.freshnessAssertionInfo} />}
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
                    {record.platform?.urn !== UNKNOWN_DATA_PLATFORM && (
                        <Tooltip
                            title={
                                record.platform.properties?.displayName ||
                                capitalizeFirstLetterOnly(record.platform.name)
                            }
                        >
                            <PlatformContainer>
                                {(record.platform.properties?.logoUrl && (
                                    <LinkWrapper
                                        to={record.lastExecUrl}
                                        target="_blank"
                                        onClick={(e) => e.stopPropagation()}
                                    >
                                        <Image
                                            preview={false}
                                            height={20}
                                            width={20}
                                            src={record.platform.properties?.logoUrl}
                                        />
                                    </LinkWrapper>
                                )) || (
                                    <Typography.Text>
                                        {record.platform.properties?.displayName ||
                                            capitalizeFirstLetterOnly(record.platform.name)}
                                    </Typography.Text>
                                )}
                            </PlatformContainer>
                        </Tooltip>
                    )}
                    <Dropdown
                        overlay={
                            <Menu>
                                <Menu.Item key="0" onClick={() => onManageAssertion(record.assertion)}>
                                    <SettingOutlined style={{ marginRight: 8 }} />
                                    Manage...
                                </Menu.Item>
                                <Menu.Item key="1" onClick={() => onDeleteAssertion(record.urn)}>
                                    <DeleteOutlined style={{ marginRight: 8 }} />
                                    Delete
                                </Menu.Item>
                            </Menu>
                        }
                        trigger={['click']}
                    >
                        <MoreOutlined style={{ fontSize: 18 }} />
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
            {managingAssertion && (
                <AssertionActionsBuilderModal
                    urn={managingAssertion.urn}
                    assertion={managingAssertion}
                    onSubmit={onUpdateAssertion}
                    onCancel={() => setManagingAssertion(undefined)}
                />
            )}
        </>
    );
};
