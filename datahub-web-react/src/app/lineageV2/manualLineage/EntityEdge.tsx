import { CloseOutlined } from '@ant-design/icons';
import Text from 'antd/lib/typography/Text';
import React from 'react';
import styled from 'styled-components/macro';
import { CorpUser, Entity } from '../../../types.generated';
import { ANTD_GRAY, DEFAULT_SYSTEM_ACTOR_URNS } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import UserAvatar from './UserAvatar';

const EntityItem = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    display: flex;
    align-items: center;
    padding: 12px 0;
    justify-content: space-between;
`;
const PlatformLogo = styled.img`
    height: 16px;
    margin-right: 8px;
`;

const NameAndLogoWrapper = styled.span`
    display: flex;
    align-items: center;
    max-width: 85%;
`;

const StyledClose = styled(CloseOutlined)`
    cursor: pointer;
`;

const EntityName = styled(Text)`
    font-size: 14px;
    font-weight: bold;
`;

interface Props {
    entity: Entity;
    removeEntity: (removedEntity: Entity) => void;
    createdOn?: number;
    createdActor?: CorpUser;
}

export default function EntityEdge({ entity, removeEntity, createdOn, createdActor }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
    const shouldDisplayAvatar =
        createdActor && !DEFAULT_SYSTEM_ACTOR_URNS.includes(createdActor.urn) && createdActor.properties !== null;

    return (
        <EntityItem data-testid="lineage-entity-item">
            <NameAndLogoWrapper>
                {platformLogoUrl && <PlatformLogo src={platformLogoUrl} alt="platform logo" />}{' '}
                <EntityName ellipsis={{ tooltip: entityRegistry.getDisplayName(entity.type, entity) }}>
                    {entityRegistry.getDisplayName(entity.type, entity)}
                </EntityName>
            </NameAndLogoWrapper>
            <span>
                {shouldDisplayAvatar && <UserAvatar createdActor={createdActor} createdOn={createdOn} />}
                <StyledClose onClick={() => removeEntity(entity)} />
            </span>
        </EntityItem>
    );
}
