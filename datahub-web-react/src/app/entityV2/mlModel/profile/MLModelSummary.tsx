import { Pill } from '@components';
import { Space, Table, Tabs, Typography } from 'antd';
import React, { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { ColorValues } from '@components/theme/config';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { notEmpty } from '@app/entityV2/shared/utils';
import { TimestampPopover } from '@app/sharedV2/TimestampPopover';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { colors } from '@src/alchemy-components/theme';

import { GetMlModelQuery } from '@graphql/mlModel.generated';
import { EntityType, MlHyperParam, MlMetric } from '@types';

const TabContent = styled.div`
    padding: 16px;
`;

const InfoItemContainer = styled.div<{ justifyContent }>`
    display: flex;
    position: relative;
    justify-content: ${(props) => props.justifyContent};
    padding: 0px 2px;
`;

const InfoItemContent = styled.div`
    padding-top: 8px;
    width: 100px;
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
`;

const JobLink = styled(Link)`
    color: ${colors.blue[700]};
    &:hover {
        text-decoration: underline;
    }
`;

const FormattedJson = styled.pre`
    margin: 0;
    padding: 8px;
    background-color: #f5f5f5;
    border-radius: 4px;
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', 'source-code-pro', monospace;
    white-space: pre-wrap;
    word-wrap: break-word;
    line-height: 1.4;
`;

const TruncatedItems = styled.div`
    position: relative;
`;

const ItemsContent = styled.div<{ isExpanded: boolean }>`
    ${({ isExpanded }) =>
        !isExpanded &&
        `
        max-height: calc(1.4em * 3 + 16px);
        overflow: hidden;
    `}
`;

const ReadMoreLink = styled.div`
    display: block;
    margin-top: 4px;
`;

export default function MLModelSummary() {
    const baseEntity = useBaseEntity<GetMlModelQuery>();
    const model = baseEntity?.mlModel;
    const entityRegistry = useEntityRegistry();
    const [expandedItemsRows, setExpandedItemsRows] = useState<Set<string>>(new Set());

    const propertyTableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            width: 450,
        },
        {
            title: 'Value',
            dataIndex: 'value',
        },
    ];

    const renderTypePill = (type: string | object) => {
        if (!type) return '-';
        const typeLabel = typeof type === 'object' && type !== null ? JSON.stringify(type) : String(type);
        return <Pill label={typeLabel} color={ColorValues.gray} variant="filled" clickable={false} />;
    };

    const renderRequiredPill = (required: boolean | undefined) => {
        if (required === undefined) return '-';
        return (
            <Pill
                label={required ? 'True' : 'False'}
                color={required ? ColorValues.blue : ColorValues.red}
                variant="filled"
                clickable={false}
            />
        );
    };

    const renderItems = (items: any, record: any, index: number) => {
        if (!items) return '-';

        const itemsJson = JSON.stringify(items, null, 2);
        const rowKey = `${record?.name || 'item'}-${index}`;
        const isExpanded = expandedItemsRows.has(rowKey);
        const isLong = itemsJson.length > 200;

        if (!isLong) {
            return <FormattedJson>{itemsJson}</FormattedJson>;
        }

        return (
            <TruncatedItems>
                <ItemsContent isExpanded={isExpanded}>
                    <FormattedJson>{itemsJson}</FormattedJson>
                </ItemsContent>
                <ReadMoreLink>
                    <Typography.Link
                        onClick={(e) => {
                            e.stopPropagation();
                            const newExpanded = new Set(expandedItemsRows);
                            if (isExpanded) {
                                newExpanded.delete(rowKey);
                            } else {
                                newExpanded.add(rowKey);
                            }
                            setExpandedItemsRows(newExpanded);
                        }}
                    >
                        {isExpanded ? 'Show less' : 'Read more'}
                    </Typography.Link>
                </ReadMoreLink>
            </TruncatedItems>
        );
    };

    const signatureTableColumns = [
        { title: 'Name', dataIndex: 'name', width: 200 },
        { title: 'Type', dataIndex: 'type', width: 200, render: renderTypePill },
        { title: 'Required', dataIndex: 'required', width: 100, render: renderRequiredPill },
        {
            title: 'Items',
            dataIndex: 'items',
            width: 300,
            render: (items, record, index) => renderItems(items, record, index),
        },
    ];

    // Parse signature data and create tabs
    const signatureData = useMemo(() => {
        const customProperties = model?.properties?.customProperties || [];

        const transformItem = (item: any, index: number) => {
            // Special handling for tensor type
            if (item.type === 'tensor' && item['tensor-spec']) {
                return {
                    name: 'tensor',
                    type: item['tensor-spec'],
                    required: item.required,
                    items: item.items,
                };
            }

            return {
                name: item.name || `Item ${index + 1}`,
                type: item.type || '-',
                required: item.required,
                items: item.items,
            };
        };

        const getSignatureData = (key: string) => {
            const property = customProperties.find((prop) => prop.key === key);
            if (!property?.value) return null;

            try {
                const parsed = JSON.parse(property.value);

                if (Array.isArray(parsed)) {
                    return parsed.map(transformItem);
                }

                if (typeof parsed === 'object' && parsed !== null) {
                    return Object.entries(parsed).map(([name, value]) => {
                        const valueObj = typeof value === 'object' && value !== null ? value : {};
                        return {
                            name,
                            type: 'type' in valueObj ? String(valueObj.type) : '-',
                            required: 'required' in valueObj ? valueObj.required : undefined,
                            items: 'items' in valueObj ? valueObj.items : undefined,
                        };
                    });
                }

                return [{ name: key, type: typeof parsed, required: undefined, items: undefined }];
            } catch (e) {
                return [{ name: key, type: '-', required: undefined, items: undefined }];
            }
        };

        return {
            inputs: getSignatureData('signature.inputs'),
            outputs: getSignatureData('signature.outputs'),
            parameters: getSignatureData('signature.parameters'),
        };
    }, [model?.properties?.customProperties]);

    const hasSignatureData = useMemo(() => {
        return Object.values(signatureData).some((data) => data && data.length > 0);
    }, [signatureData]);

    const renderModelSignature = () => {
        const tabConfigs = [
            { key: 'inputs', label: 'Inputs', data: signatureData.inputs, rowKeyPrefix: 'input' },
            { key: 'outputs', label: 'Outputs', data: signatureData.outputs, rowKeyPrefix: 'output' },
            { key: 'parameters', label: 'Parameters', data: signatureData.parameters, rowKeyPrefix: 'parameter' },
        ];

        const tabs = tabConfigs
            .filter((config) => config.data && config.data.length > 0)
            .map((config) => ({
                key: config.key,
                label: config.label,
                children: (
                    <Table
                        pagination={false}
                        columns={signatureTableColumns}
                        dataSource={config.data || []}
                        rowKey={(record, index) => `${config.rowKeyPrefix}-${index}`}
                    />
                ),
            }));

        return tabs.length > 0 ? <Tabs items={tabs} /> : null;
    };

    const renderTrainingJobs = () => {
        const trainingJobs =
            model?.trainedBy?.relationships?.map((relationship) => relationship.entity).filter(notEmpty) || [];

        if (trainingJobs.length === 0) return '-';

        return (
            <div>
                {trainingJobs.map((job, index) => {
                    const { urn, name } = job as { urn: string; name?: string };
                    return (
                        <span key={urn}>
                            <JobLink to={entityRegistry.getEntityUrl(EntityType.DataProcessInstance, urn)}>
                                {name || urn}
                            </JobLink>
                            {index < trainingJobs.length - 1 && ', '}
                        </span>
                    );
                })}
            </div>
        );
    };

    return (
        <TabContent>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <Typography.Title level={3}>Model Details</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Version">
                        <InfoItemContent>{model?.versionProperties?.version?.versionTag}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Registered At">
                        <TimestampPopover timestamp={model?.properties?.created?.time} title="Registered At" />
                    </InfoItem>
                    <InfoItem title="Last Modified At">
                        <TimestampPopover timestamp={model?.properties?.lastModified?.time} title="Last Modified At" />
                    </InfoItem>
                    <InfoItem title="Created By">
                        <InfoItemContent>{model?.properties?.created?.actor || '-'}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Aliases">
                        <InfoItemContent>
                            {model?.versionProperties?.aliases?.map((alias) => (
                                <Pill
                                    label={alias.versionTag ?? '-'}
                                    key={alias.versionTag}
                                    color="blue"
                                    clickable={false}
                                />
                            ))}
                        </InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Source Run">
                        <InfoItemContent>{renderTrainingJobs()}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <Typography.Title level={3}>Training Metrics</Typography.Title>
                <Table
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={model?.properties?.trainingMetrics as MlMetric[]}
                />
                <Typography.Title level={3}>Hyper Parameters</Typography.Title>
                <Table
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={model?.properties?.hyperParams as MlHyperParam[]}
                />
                {hasSignatureData && (
                    <>
                        <Typography.Title level={3}>Model Signature</Typography.Title>
                        {renderModelSignature()}
                    </>
                )}
            </Space>
        </TabContent>
    );
}
