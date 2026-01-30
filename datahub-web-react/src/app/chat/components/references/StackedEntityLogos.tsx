import { colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { deduplicateEntitiesByPlatform } from '@app/chat/utils/deduplicateEntitiesByPlatform';
import { IconStyleType } from '@app/entityV2/Entity';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpUser, Domain, Entity, EntityType } from '@types';

const CIRCLE_SIZE = 24;
const LOGO_SIZE = 16;

const StackContainer = styled.div`
    display: flex;
    align-items: center;
`;

const LogoWrapper = styled.div`
    margin-left: -8px;

    &:first-child {
        margin-left: 0;
    }
`;

const LogoCircle = styled.div`
    width: ${CIRCLE_SIZE}px;
    height: ${CIRCLE_SIZE}px;
    border-radius: 50%;
    background-color: ${colors.gray[1500]};
    border: 2px solid white;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    flex-shrink: 0;
`;

const LogoImage = styled.img`
    width: ${LOGO_SIZE}px;
    height: ${LOGO_SIZE}px;
    object-fit: contain;
    flex-shrink: 0;
`;

const AvatarImage = styled.img`
    width: 100%;
    height: 100%;
    object-fit: cover;
    border-radius: 50%;
`;

const FallbackIcon = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    color: ${colors.gray[600]};
    line-height: 0;
`;

const MoreIndicator = styled.div`
    width: ${CIRCLE_SIZE}px;
    height: ${CIRCLE_SIZE}px;
    border-radius: 50%;
    background-color: ${colors.gray[1500]};
    border: 2px solid white;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 10px;
    font-weight: 600;
    color: ${colors.gray[1700]};
    flex-shrink: 0;
`;

const DomainIconWrapper = styled.div`
    border: 2px solid white;
    border-radius: 50%;
    overflow: hidden;
    display: flex;
    align-items: center;
    justify-content: center;
`;

interface LogoItemProps {
    entity: Entity;
}

const LogoItem: React.FC<LogoItemProps> = ({ entity }) => {
    const [hasError, setHasError] = useState(false);
    const entityRegistry = useEntityRegistryV2();

    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const logoUrl = properties?.platform?.properties?.logoUrl;

    // Handle domains specially - they have custom colored icons
    // Only use DomainColoredIcon if displayProperties is loaded, otherwise fall back to generic icon
    if (entity.type === EntityType.Domain) {
        const domain = entity as Domain;
        const hasDisplayProperties = domain.displayProperties || domain.properties?.name;

        if (hasDisplayProperties) {
            return (
                <LogoWrapper>
                    <DomainIconWrapper>
                        <DomainColoredIcon domain={domain} size={16} fontSize={10} />
                    </DomainIconWrapper>
                </LogoWrapper>
            );
        }
        // Fall through to use generic icon if displayProperties not loaded
    }

    // Handle users - they have avatar photos
    if (entity.type === EntityType.CorpUser) {
        const user = entity as CorpUser;
        const avatarUrl = user.editableProperties?.pictureLink;

        return (
            <LogoWrapper>
                <LogoCircle>
                    {avatarUrl && !hasError ? (
                        <AvatarImage src={avatarUrl} alt="" onError={() => setHasError(true)} />
                    ) : (
                        <FallbackIcon>
                            {entityRegistry.getIcon(entity.type, 14, IconStyleType.ACCENT, colors.gray[1700])}
                        </FallbackIcon>
                    )}
                </LogoCircle>
            </LogoWrapper>
        );
    }

    return (
        <LogoWrapper>
            <LogoCircle>
                {logoUrl && !hasError ? (
                    <LogoImage src={logoUrl} alt="" onError={() => setHasError(true)} />
                ) : (
                    <FallbackIcon>
                        {entityRegistry.getIcon(entity.type, 14, IconStyleType.ACCENT, colors.gray[1700])}
                    </FallbackIcon>
                )}
            </LogoCircle>
        </LogoWrapper>
    );
};

interface Props {
    entities: Entity[];
    maxDisplay?: number;
}

export const StackedEntityLogos: React.FC<Props> = ({ entities, maxDisplay = 4 }) => {
    if (entities.length === 0) {
        return null;
    }

    const uniqueEntities = deduplicateEntitiesByPlatform(entities);
    const displayEntities = uniqueEntities.slice(0, maxDisplay);
    const remainingCount = uniqueEntities.length - maxDisplay;

    return (
        <StackContainer>
            {displayEntities.map((entity) => (
                <LogoItem key={entity.urn} entity={entity} />
            ))}
            {remainingCount > 0 && (
                <LogoWrapper>
                    <MoreIndicator>+{remainingCount}</MoreIndicator>
                </LogoWrapper>
            )}
        </StackContainer>
    );
};
