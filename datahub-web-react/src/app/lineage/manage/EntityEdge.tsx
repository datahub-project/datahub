import { CloseOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components/macro';
import { CorpUser, Entity } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityName } from './LineageEntityView';
import UserAvatar from './UserAvatar';

const DEFAULT_SYSTEM_ACTOR_URNS = ['urn:li:corpuser:__datahub_system', 'urn:li:corpuser:unknown'];

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
`;

const StyledClose = styled(CloseOutlined)`
    cursor: pointer;
`;

interface Props {
    entity: Entity;
    removeEntity: (removedEntity: Entity) => void;
    createdActor?: CorpUser | null;
    createdOn?: number | null;
}

export default function EntityEdge({ entity, removeEntity, createdActor, createdOn }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
    const shouldDisplayAvatar =
        createdActor && !DEFAULT_SYSTEM_ACTOR_URNS.includes(createdActor.urn) && createdActor.properties !== null;

    return (
        <EntityItem>
            <NameAndLogoWrapper>
                {platformLogoUrl && <PlatformLogo src={platformLogoUrl} alt="platform logo" />}{' '}
                <EntityName>{entityRegistry.getDisplayName(entity.type, entity)}</EntityName>
            </NameAndLogoWrapper>
            <span>
                {shouldDisplayAvatar && <UserAvatar createdActor={createdActor} createdOn={createdOn} />}
                <StyledClose onClick={() => removeEntity(entity)} />
            </span>
        </EntityItem>
    );
}
