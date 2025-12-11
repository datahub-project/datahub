/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Space, Table, Typography } from 'antd';
import { ColumnsType } from 'antd/es/table';
import Link from 'antd/lib/typography/Link';
import React from 'react';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetMlModelQuery } from '@graphql/mlModel.generated';
import { EntityType, MlModelGroup } from '@types';

const TabContent = styled.div`
    padding: 16px;
`;

export default function MLModelGroupsTab() {
    const baseEntity = useBaseEntity<GetMlModelQuery>();
    const model = baseEntity?.mlModel;

    const entityRegistry = useEntityRegistry();

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
