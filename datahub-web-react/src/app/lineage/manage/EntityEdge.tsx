import { CloseOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityName } from './LineageEntityView';

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

const StyledClose = styled(CloseOutlined)`
    cursor: pointer;
`;

interface Props {
    entity: Entity;
    removeEntity: (removedEntity: Entity) => void;
}

export default function EntityEdge({ entity, removeEntity }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;

    return (
        <EntityItem data-testid="lineage-entity-item">
            <span>
                {platformLogoUrl && <PlatformLogo src={platformLogoUrl} alt="platform logo" />}{' '}
                <EntityName>{entityRegistry.getDisplayName(entity.type, entity)}</EntityName>
            </span>
            <StyledClose onClick={() => removeEntity(entity)} />
        </EntityItem>
    );
}
