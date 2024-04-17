import React from 'react';
import { List } from 'antd';
import styled from 'styled-components';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { PreviewType } from '../../../../Entity';
import { EntityType } from '../../../../../../types.generated';

const StyledList = styled(List)`
    padding-left: 40px;
    padding-right: 40px;
    .ant-list-items > .ant-list-item {
        padding-right: 0px;
        padding-left: 0px;
    }
    > .ant-list-header {
        padding-right: 0px;
        padding-left: 0px;
        font-size: 14px;
        font-weight: 600;
        margin-left: -20px;
        border-bottom: none;
        padding-bottom: 0px;
        padding-top: 15px;
    }
` as typeof List;

const StyledListItem = styled(List.Item)`
    padding-top: 20px;
`;

type EntityListProps = {
    type: EntityType;
    entities: Array<any>;
    title?: string;
};

export const EntityList = ({ type, entities, title }: EntityListProps) => {
    const entityRegistry = useEntityRegistry();
    return (
        <StyledList
            bordered
            dataSource={entities}
            header={title || `${entities.length || 0} ${entityRegistry.getCollectionName(type)}`}
            renderItem={(item) => (
                <StyledListItem>{entityRegistry.renderPreview(type, PreviewType.PREVIEW, item)}</StyledListItem>
            )}
        />
    );
};
