import React from 'react';
import { List, Typography, Divider } from 'antd';
import styled from 'styled-components';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../../entity/Entity';
import { EntityType, SearchResult } from '../../../types.generated';

type Props = {
    searchResult: {
        [key in EntityType]?: Array<SearchResult>;
    };
    entityPath?: string;
};

const ListContainer = styled.div`
    display: default;
    flex-grow: default;
`;

const TitleContainer = styled.div`
    margin-bottom: 30px;
`;

export default ({ searchResult, entityPath }: Props) => {
    const entityRegistry = useEntityRegistry();
    const entityType = entityRegistry.getTypeFromPathName(entityPath || '');
    if (!entityType) return null;

    const entitiesToShow = searchResult[entityType] || [];

    return (
        <ListContainer>
            <TitleContainer>
                <Typography.Title level={3}>{entityRegistry.getCollectionName(entityType)}</Typography.Title>
                <Divider />
            </TitleContainer>
            <List
                dataSource={entitiesToShow}
                renderItem={(item) => {
                    return (
                        <>
                            {entityRegistry.renderPreview(entityType, PreviewType.PREVIEW, item.entity)}
                            <Divider />
                        </>
                    );
                }}
            />
        </ListContainer>
    );
};
