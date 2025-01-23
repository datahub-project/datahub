import { Button, Dropdown, Empty, Image, message, Modal, Tag, Tooltip, Typography, Checkbox } from 'antd';
import React from 'react';
import styled from 'styled-components';
import {
    DeleteOutlined,
    DownOutlined,
    MoreOutlined,
    RightOutlined,
    StopOutlined,
    AuditOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { DatasetAssertionDescription } from './DatasetAssertionDescription';
import { StyledTable } from '../../../components/styled/StyledTable';
import { DatasetAssertionDetails } from './DatasetAssertionDetails';
import { Assertion, AssertionRunStatus, DataContract, EntityType } from '../../../../../../types.generated';
import { getResultColor, getResultIcon, getResultText } from './assertionUtils';
import { useDeleteAssertionMutation } from '../../../../../../graphql/assertion.generated';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import { REDESIGN_COLORS } from '../../../constants';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { isAssertionPartOfContract } from './contract/utils';
import { useEntityData } from '../../../EntityContext';
import CopyUrnMenuItem from '../../../../../shared/share/items/CopyUrnMenuItem';

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

const AssertionSelectCheckbox = styled(Checkbox)`
    margin-right: 12px;
`;

const DataContractLogo = styled(AuditOutlined)`
    margin-left: 8px;
    font-size: 16px;
    color: ${REDESIGN_COLORS.BLUE};
`;

const AssertionDescriptionContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

type Props = {
    assertions: Array<Assertion>;
    onDelete?: (urn: string) => void;
    contract?: DataContract;
    // required for enabling menu/actions
    showMenu?: boolean;
    onSelect?: (assertionUrn: string) => void;
    // required for enabling selection logic
    showSelect?: boolean;
    selectedUrns?: string[];
};

/**
 * A list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 *
 * Currently this component supports rendering Dataset Assertions only.
 */
export const DatasetAssertionsList = ({
    assertions,
    onDelete,
    showMenu = true,
    showSelect,
    onSelect,
    selectedUrns,
    contract,
}: Props) => {
    const entityData = useEntityData();
    const [deleteAssertionMutation] = useDeleteAssertionMutation();
    const entityRegistry = useEntityRegistry();

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

    const assertionMenuItems = (urn: string) => {
        return [
            {
                key: 1,
                label: <CopyUrnMenuItem key="1" urn={urn} type="Assertion" />,
            },
        ];
    };

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
                const selected = selectedUrns?.some((selectedUrn) => selectedUrn === record.urn);
                const isPartOfContract = contract && isAssertionPartOfContract(record, contract);

                const { description } = record;
                return (
                    <ResultContainer>
                        {showSelect ? (
                            <AssertionSelectCheckbox
                                checked={selected}
                                onClick={(e) => e.stopPropagation()}
                                onChange={() => onSelect?.(record.urn as string)}
                            />
                        ) : undefined}
                        <div>
                            <Tooltip title={(localTime && `Last evaluated on ${localTime}`) || 'No Evaluations'}>
                                <Tag style={{ borderColor: resultColor }}>
                                    {resultIcon}
                                    <ResultTypeText style={{ color: resultColor }}>{resultText}</ResultTypeText>
                                </Tag>
                            </Tooltip>
                        </div>
                        {record.datasetAssertionInfo ? (
                            <DatasetAssertionDescription
                                description={description}
                                assertionInfo={record.datasetAssertionInfo}
                            />
                        ) : (
                            <AssertionDescriptionContainer>
                                {description ?? 'No description provided'}
                            </AssertionDescriptionContainer>
                        )}

                        {(isPartOfContract && entityData?.urn && (
                            <Tooltip
                                title={
                                    <>
                                        Part of Data Contract{' '}
                                        <Link
                                            to={`${entityRegistry.getEntityUrl(
                                                EntityType.Dataset,
                                                entityData.urn,
                                            )}/Quality/Data Contract`}
                                            style={{ color: REDESIGN_COLORS.BLUE }}
                                        >
                                            view
                                        </Link>
                                    </>
                                }
                            >
                                <Link
                                    to={`${entityRegistry.getEntityUrl(
                                        EntityType.Dataset,
                                        entityData.urn,
                                    )}/Quality/Data Contract`}
                                >
                                    <DataContractLogo />
                                </Link>
                            </Tooltip>
                        )) ||
                            undefined}
                    </ResultContainer>
                );
            },
        },
        {
            title: '',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                return (
                    showMenu && (
                        <ActionButtonContainer>
                            <Tooltip
                                title={
                                    record.platform.properties?.displayName ||
                                    capitalizeFirstLetterOnly(record.platform.name)
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
                            <Dropdown menu={{ items: assertionMenuItems(record.urn) }} trigger={['click']}>
                                <StyledMoreOutlined />
                            </Dropdown>
                        </ActionButtonContainer>
                    )
                );
            },
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
                expandable={
                    showSelect
                        ? {}
                        : {
                              defaultExpandAllRows: false,
                              expandRowByClick: true,
                              expandedRowRender: (record) => {
                                  return (
                                      <DatasetAssertionDetails
                                          urn={record.urn}
                                          lastEvaluatedAtMillis={record.lastExecTime}
                                      />
                                  );
                              },
                              expandIcon: ({ expanded, onExpand, record }: any) =>
                                  expanded ? (
                                      <DownOutlined style={{ fontSize: 8 }} onClick={(e) => onExpand(record, e)} />
                                  ) : (
                                      <RightOutlined style={{ fontSize: 8 }} onClick={(e) => onExpand(record, e)} />
                                  ),
                          }
                }
                onRow={(record) => {
                    return {
                        onClick: (_) => {
                            if (showSelect) {
                                onSelect?.(record.urn as string);
                            }
                        },
                    };
                }}
                showHeader={false}
                pagination={false}
            />
        </>
    );
};
