import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { GenericEntityProperties } from '../../../entity/shared/types';
import { Entity, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getDisplayedEntityType } from '../../../entityV2/shared/containers/profile/header/utils';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { SEARCH_COLORS } from '../../../entityV2/shared/constants';
import PlatformIcon from '../../../sharedV2/icons/PlatformIcon';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    padding: 10px 12px 10px 12px;
    background-color: #ffffff;
    border-radius: 10px;
    min-width: 200px;
    max-width: 260px;
    border: 1.5px solid #0000001a;
    gap: 12px;

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
    gap: 8px;
`;

const SubHeader = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[5]};
    margin-top: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const Type = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
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
