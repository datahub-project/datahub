import { Space, Table, Typography } from 'antd';
import { ColumnsType } from 'antd/es/table';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetMlModelQuery } from '@graphql/mlModel.generated';
import { EntityType, MlModelGroup } from '@types';

const TabContent = styled.div`
    padding: 16px;
`;

const TruncatedDescription = styled.div<{ isExpanded: boolean }>`
    display: -webkit-box;
    -webkit-line-clamp: ${({ isExpanded }) => (isExpanded ? 'unset' : '3')};
    -webkit-box-orient: vertical;
    overflow: hidden;
`;

export default function MLModelGroupsTab() {
    const baseEntity = useBaseEntity<GetMlModelQuery>();
    const model = baseEntity?.mlModel;
    const entityRegistry = useEntityRegistry();
    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
    const { t } = useTranslation('entity.types');
    const { t: tl } = useTranslation('common.labels');
    const { t: tc } = useTranslation('common.actions');

    const propertyTableColumns: ColumnsType<MlModelGroup> = [
        {
            title: t('group.name'),
            dataIndex: 'name',
            render: (name, record) => {
                return (
                    <div data-testid={`model-group-name-${record.properties?.name || name}`}>
                        <Link to={entityRegistry.getEntityUrl(EntityType.MlmodelGroup, record.urn)}>
                            {record.properties?.name || name}
                        </Link>
                    </div>
                );
            },
        },
        {
            title: tl('description'),
            dataIndex: 'description',
            render: (_, record) => {
                const editableDesc = record.editableProperties?.description;
                const originalDesc = record.description;
                const description = editableDesc || originalDesc;

                if (!description) return '-';

                const isExpanded = expandedRows.has(record.urn);
                const isLong = description.length > 150;

                if (!isLong) return description;

                return (
                    <>
                        <TruncatedDescription isExpanded={isExpanded}>{description}</TruncatedDescription>
                        <Typography.Link
                            onClick={() => {
                                const newExpanded = new Set(expandedRows);
                                if (isExpanded) {
                                    newExpanded.delete(record.urn);
                                } else {
                                    newExpanded.add(record.urn);
                                }
                                setExpandedRows(newExpanded);
                            }}
                        >
                            {isExpanded ? tc('showLess') : tc('readMore')}
                        </Typography.Link>
                    </>
                );
            },
        },
    ];

    return (
        <TabContent>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <Typography.Title level={3}>{t('group.namePlural')}</Typography.Title>
                <Table
                    data-testid="mlmodel-groups-table"
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={model?.properties?.groups as MlModelGroup[]}
                />
            </Space>
        </TabContent>
    );
}
