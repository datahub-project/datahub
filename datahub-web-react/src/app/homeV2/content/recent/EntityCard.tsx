import React from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { GenericEntityProperties } from '../../../entityV2/shared/types';
import { Entity, EntityType } from '../../../../types.generated';
import { IconStyleType } from '../../../entityV2/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getDisplayedEntityType } from '../../../entityV2/shared/containers/profile/header/utils';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { navigateToEntityProfile } from '../../shared/navigateToEntityProfile';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { SEARCH_COLORS } from '../../../entityV2/shared/constants';

const Card = styled.div`
    :hover {
        cursor: pointer;
    }
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    padding: 10px 20px 12px 20px;
    background-color: #ffffff;
    border-radius: 10px;
    min-width: 200px;
    max-width: 260px;
    border: 1.5px solid #0000001a;
    :hover {
        border: 1.5px solid ${SEARCH_COLORS.LINK_BLUE};
    }
`;

const Name = styled.div`
    font-size: 14px;
    color: #565657;
    margin-bottom: 4px;
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Context = styled.div`
    display: flex;
    align-items: center;
`;

const SubHeader = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[5]};
    margin-top: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const Icon = styled.div`
    margin-right: 8px;
    border-radius: 4px;
    background-color: ${ANTD_GRAY[3]};
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 6px;
    width: 24px;
    height: 24px;
`;

const Type = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
    text-overflow: ellipsis;
    overflow: hidden;
    white-space: nowrap;
`;

const PreviewImage = styled.img`
    height: 14px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

type Props = {
    entity: Entity;
    subHeader?: React.ReactNode;
    render?: (entity: Entity) => React.ReactNode;
    className?: string;
};

export const EntityCard = ({ entity, subHeader, render, className }: Props) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(entity?.type as EntityType, entity);
    const displayType = getDisplayedEntityType(entity, entityRegistry, entity.type);

    const defaultRender = (e: GenericEntityProperties) => {
        const logo = e.platform?.properties?.logoUrl ? (
            <PreviewImage src={e.platform?.properties?.logoUrl || undefined} alt={displayName} />
        ) : (
            entityRegistry.getIcon(e.type as EntityType, 12, IconStyleType.ACCENT)
        );
        return (
            <HoverEntityTooltip placement="bottom" entity={entity} showArrow={false}>
                <Container className={className}>
                    <Name>{displayName}</Name>
                    <Context>
                        <Icon>{logo}</Icon>
                        <Type>{displayType}</Type>
                    </Context>
                    {subHeader && <SubHeader>{subHeader}</SubHeader>}
                </Container>
            </HoverEntityTooltip>
        );
    };

    return (
        <Card onClick={() => navigateToEntityProfile(history, entityRegistry, entity)}>
            {render ? render(entity) : defaultRender(entity)}
        </Card>
    );
};
