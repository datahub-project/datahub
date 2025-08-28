import { Space, Table, Typography } from 'antd';
import { ColumnsType } from 'antd/es/table';
import Link from 'antd/lib/typography/Link';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import StripMarkdownText from '@app/entity/shared/components/styled/StripMarkdownText';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetMlModelQuery } from '@graphql/mlModel.generated';
import { EntityType, MlModelGroup } from '@types';

const TabContent = styled.div`
    padding: 16px;
`;

export default function MLModelGroupsTab() {
    const baseEntity = useBaseEntity<GetMlModelQuery>();
    const model = baseEntity?.mlModel;
    const [expandedDescriptions, setExpandedDescriptions] = useState<Set<string>>(new Set());

    const entityRegistry = useEntityRegistry();

    const ABBREVIATED_LIMIT = 80;

    const handleExpanded = (urn: string, expanded: boolean) => {
        const newExpanded = new Set(expandedDescriptions);
        if (expanded) {
            newExpanded.add(urn);
        } else {
            newExpanded.delete(urn);
        }
        setExpandedDescriptions(newExpanded);
    };

    const propertyTableColumns: ColumnsType<MlModelGroup> = [
        {
            title: 'Group',
            dataIndex: 'name',
            render: (name, record) => {
                return (
                    <Link href={entityRegistry.getEntityUrl(EntityType.MlmodelGroup, record.urn)}>
                        {record.properties?.name || name}
                    </Link>
                );
            },
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            width: 300,
            render: (_: any, record: any) => {
                const description = record.description || '';

                if (!description) {
                    return <Typography.Text>-</Typography.Text>;
                }

                const isExpanded = expandedDescriptions.has(record.urn);

                if (isExpanded) {
                    return (
                        <>
                            <Typography.Text>{description}</Typography.Text>
                            <br />
                            <Typography.Link
                                onClick={(e) => {
                                    e.stopPropagation();
                                    handleExpanded(record.urn, false);
                                }}
                            >
                                Read Less
                            </Typography.Link>
                        </>
                    );
                }

                return (
                    <StripMarkdownText
                        limit={ABBREVIATED_LIMIT}
                        readMore={
                            <>
                                <Typography.Link
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        handleExpanded(record.urn, true);
                                    }}
                                >
                                    Read More
                                </Typography.Link>
                            </>
                        }
                        shouldWrap
                    >
                        {description}
                    </StripMarkdownText>
                );
            },
        },
    ];

    return (
        <TabContent>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <Typography.Title level={3}>Groups</Typography.Title>
                <Table
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={model?.properties?.groups as MlModelGroup[]}
                />
            </Space>
        </TabContent>
    );
}
