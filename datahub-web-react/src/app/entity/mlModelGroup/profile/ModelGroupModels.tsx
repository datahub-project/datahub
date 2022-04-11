import { List, Space, Typography } from 'antd';
import React from 'react';
import { GetMlModelGroupQuery } from '../../../../graphql/mlModelGroup.generated';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';
import { useBaseEntity } from '../../shared/EntityContext';

export default function MLGroupModels() {
    const baseEntity = useBaseEntity<GetMlModelGroupQuery>();
    const models = baseEntity?.mlModelGroup?.incoming?.relationships?.map((relationship) => relationship.entity) || [];

    const entityRegistry = useEntityRegistry();

    return (
        <>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <List
                    style={{ padding: '16px 16px' }}
                    bordered
                    dataSource={models}
                    header={<Typography.Title level={3}>Models</Typography.Title>}
                    renderItem={(item) => (
                        <List.Item style={{ paddingTop: '20px' }}>
                            {entityRegistry.renderPreview(EntityType.Mlmodel, PreviewType.PREVIEW, item)}
                        </List.Item>
                    )}
                />
            </Space>
        </>
    );
}
