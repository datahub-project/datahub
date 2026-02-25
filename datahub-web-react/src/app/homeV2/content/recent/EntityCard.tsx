import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { getDisplayedEntityType } from '@app/entityV2/shared/containers/profile/header/utils';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    padding: 10px 12px 10px 12px;
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-radius: 10px;
    min-width: 200px;
    max-width: 260px;
    border: 1.5px solid ${(props) => props.theme.colors.border};
    gap: 12px;

    :hover {
        border: 1.5px solid ${(props) => props.theme.colors.hyperlinks};
    }
`;

const Name = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
    margin-bottom: 4px;
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Context = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const SubHeader = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.border};
    margin-top: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const Type = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
    text-overflow: ellipsis;
    overflow: hidden;
    white-space: nowrap;
`;

const Text = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 140px;
`;

type Props = {
    entity: Entity;
    subHeader?: React.ReactNode;
    render?: (entity: Entity) => React.ReactNode;
    className?: string;
};

export const EntityCard = ({ entity, subHeader, render, className }: Props) => {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(entity?.type as EntityType, entity);
    const displayType = getDisplayedEntityType(entity, entityRegistry, entity.type);

    const defaultRender = (e: GenericEntityProperties) => {
        return (
            <HoverEntityTooltip placement="bottom" entity={entity} showArrow={false}>
                <Container className={className}>
                    <Context>
                        <PlatformIcon
                            platform={e?.platform}
                            size={28}
                            alt={displayName}
                            entityType={e.type as EntityType}
                        />
                    </Context>
                    <Text>
                        <Type>{displayType}</Type>
                        <Name>{displayName}</Name>
                        {subHeader && <SubHeader>{subHeader}</SubHeader>}
                    </Text>
                </Container>
            </HoverEntityTooltip>
        );
    };

    return (
        <Link to={entityRegistry.getEntityUrl(entity.type, entity.urn)}>
            {render ? render(entity) : defaultRender(entity)}
        </Link>
    );
};
