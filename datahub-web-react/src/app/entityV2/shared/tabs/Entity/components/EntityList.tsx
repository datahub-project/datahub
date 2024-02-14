import React from 'react';
import { List } from 'antd';
import styled from 'styled-components';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { PreviewType } from '../../../../Entity';
import { EntityType } from '../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../constants';

const StyledList = styled(List)`
    padding-left: 40px;
    padding-right: 40px;
    background-color: ${REDESIGN_COLORS.BACKGROUND_GREY};
    box-shadow: 0px 0px 30px 0px rgb(239 239 239);
    .ant-list-items > .ant-list-item {
        padding-right: 0px;
        padding-left: 0px;
    }
    > .ant-list-header {
        padding-right: 0px;
        padding-left: 0px;
        font-size: 14px;
        font-weight: 600;
        border-bottom: none;
        padding-bottom: 0px;
        padding-top: 15px;
    }
` as typeof List;

const StyledListItem = styled(List.Item)`
    display: flex;
    align-items: center;
    background-color: ${REDESIGN_COLORS.WHITE};
    padding: 20px !important;
    margin: 16px 0px;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
    border-radius: 8px;
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
