import React from 'react';
import { List, Typography } from 'antd';
import styled from 'styled-components';

import { EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../Entity';

type Props = {
    ownerships: { [key in EntityType]?: any[] };
    entityPath?: string;
};

const ListContainer = styled.div`
    display: default;
    flex-grow: default;
`;

export default ({ ownerships, entityPath }: Props) => {
    const entityRegistry = useEntityRegistry();

    const entityType = entityRegistry.getTypeFromPathName(entityPath || '');

    if (!entityType) return null;

    const entitiesToShow = ownerships[entityType] || [];

    return (
        <ListContainer>
            <List
                dataSource={entitiesToShow}
                header={
                    <Typography.Title level={3}>
                        {entityRegistry.getCollectionName(entityType)} they own
                    </Typography.Title>
                }
                renderItem={(item) => {
                    return entityRegistry.renderPreview(entityType, PreviewType.PREVIEW, item);
                }}
            />
        </ListContainer>
    );
};
