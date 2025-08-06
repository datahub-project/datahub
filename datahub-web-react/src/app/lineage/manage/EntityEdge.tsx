import { CloseOutlined } from '@ant-design/icons';
import Text from 'antd/lib/typography/Text';
import React from 'react';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY, DEFAULT_SYSTEM_ACTOR_URNS } from '@app/entity/shared/constants';
import { getPlatformName } from '@app/entity/shared/utils';
import { ContainerView } from '@app/lineage/manage/ContainerView';
import UserAvatar from '@app/lineage/manage/UserAvatar';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, Entity } from '@types';

const PlatformContent = styled.div<{ removeMargin?: boolean }>`
    display: flex;
    align-items: center;
    font-size: 10px;
    margin-left: 5px;
    color: ${ANTD_GRAY[7]};
`;

const PlatformName = styled.span`
    margin-left: 0px;
`;

const EntityItemOuter = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    display: block;
    align-items: center;
    padding: 12px 0;
    justify-content: space-between;
`;

const EntityItem = styled.div`
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
    createdActor?: CorpUser | null;
    createdOn?: number | null;
}

export default function EntityEdge({ entity, removeEntity, createdActor, createdOn }: Props) {
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
    const shouldDisplayAvatar =
        createdActor && !DEFAULT_SYSTEM_ACTOR_URNS.includes(createdActor.urn) && createdActor.properties !== null;
    const containers = entityData?.parentContainers?.containers;
    const remainingContainers = containers?.slice(1);
    const directContainer = containers ? containers[0] : null;
    const platformName = getPlatformName(genericProps);
    return (
        <EntityItemOuter>
            <PlatformContent>
                <PlatformName>{platformName}</PlatformName>
                <ContainerView remainingContainers={remainingContainers} directContainer={directContainer} />
            </PlatformContent>
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
        </EntityItemOuter>
    );
}
