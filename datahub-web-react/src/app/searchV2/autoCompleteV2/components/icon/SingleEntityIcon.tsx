import React, { useState } from 'react';
import styled from 'styled-components';

import GlossaryEntityIcon from '@app/glossaryV2/GlossaryEntityIcon';
import LogicalPlatformDefaultIcon from '@app/sharedV2/logical/LogicalPlatformDefaultIcon';
import { IconStyleType } from '@src/app/entityV2/Entity';
import { getPlatformNameFromEntityData } from '@src/app/entityV2/shared/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { Entity, EntityType, GlossaryNode, GlossaryTerm } from '@src/types.generated';

const ImageIcon = styled.img<{ $size: number }>`
    vertical-align: middle;
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    object-fit: contain;
    background-color: transparent;
`;

const EntityIcon = styled.div`
    display: flex;
    color: ${(props) => props.theme.colors.icon};
`;

interface Props {
    entity: Entity;
    size: number;
}

export function SingleEntityIcon({ entity, size }: Props) {
    const [isBrokenPlatformLogoUrl, setIsBrokenPlatformLogoUrl] = useState<boolean>(false);
    const entityRegistry = useEntityRegistry();

    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformLogoUrl = properties?.platform?.properties?.logoUrl;
    const platformName = getPlatformNameFromEntityData(properties);

    if (entity.type === EntityType.GlossaryTerm || entity.type === EntityType.GlossaryNode) {
        return <GlossaryEntityIcon entity={entity as GlossaryTerm | GlossaryNode} size={size} />;
    }

    if (properties?.platform?.properties?.logical) {
        return <LogicalPlatformDefaultIcon size={size} />;
    }

    return (
        (platformLogoUrl && !isBrokenPlatformLogoUrl && (
            <ImageIcon
                src={platformLogoUrl}
                alt={platformName || ''}
                $size={size}
                onError={() => setIsBrokenPlatformLogoUrl(true)}
            />
        )) || <EntityIcon>{entityRegistry.getIcon(entity.type, size, IconStyleType.ACCENT)}</EntityIcon>
    );
}
