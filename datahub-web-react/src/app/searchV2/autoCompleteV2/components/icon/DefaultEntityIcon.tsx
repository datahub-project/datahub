import { radius } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { getEntityPlatforms } from '@app/entityV2/shared/containers/profile/header/utils';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import GlossaryEntityIcon from '@app/glossaryV2/GlossaryEntityIcon';
import { PlatformIcon } from '@app/searchV2/autoCompleteV2/components/icon/PlatformIcon';
import { SingleEntityIcon } from '@app/searchV2/autoCompleteV2/components/icon/SingleEntityIcon';
import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import useUniqueEntitiesByPlatformUrn from '@app/searchV2/autoCompleteV2/components/icon/useUniqueEntitiesByPlatformUrn';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Domain, EntityType, GlossaryNode, GlossaryTerm } from '@types';

const Container = styled.div<{ $size: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    background: ${(props) => props.theme.colors.bgSurface};
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    border-radius: ${radius.full};
`;

const DomainContainer = styled.div<{ $size: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
`;

const GlossaryContainer = styled.div<{ $size: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
`;

const IconContainer = styled.div`
    margin-left: -4px;
    &:first-child {
        margin-left: 0;
    }
`;

// Default sizing for callers that don't opt into a custom `size` — kept as separate constants
// (rather than deriving from `size`) so existing autocomplete-row callers render unchanged.
const DEFAULT_CONTAINER_SIZE = 28;
const DEFAULT_ICON_SIZE = 20;
const DEFAULT_SIBLING_ICON_SIZE = 16;
const DEFAULT_DOMAIN_ICON_SIZE = 24;
const DEFAULT_DOMAIN_FONT_SIZE = 16;
const DEFAULT_GLOSSARY_ICON_SIZE = 14;

export default function DefaultEntityIcon({ entity, siblings, size }: EntityIconProps) {
    const entityRegistry = useEntityRegistryV2();
    const uniqueSiblingsByPlatform = useUniqueEntitiesByPlatformUrn(siblings);
    const hasSiblings = useMemo(() => (uniqueSiblingsByPlatform?.length ?? 0) > 0, [uniqueSiblingsByPlatform?.length]);
    const entitiesToShowIcons = useMemo(
        () => (hasSiblings ? uniqueSiblingsByPlatform : [entity]),
        [hasSiblings, uniqueSiblingsByPlatform, entity],
    );
    // A caller-provided `size` collapses the container to a tight fit around the icon (no
    // breathing room); the default keeps the original fixed autocomplete-row proportions.
    const containerSize = size ?? DEFAULT_CONTAINER_SIZE;
    const iconSize = size ?? (hasSiblings ? DEFAULT_SIBLING_ICON_SIZE : DEFAULT_ICON_SIZE);

    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const { platforms } = getEntityPlatforms(entity.type, properties);

    if (entity.type === EntityType.Domain) {
        const domainIconSize = size ?? DEFAULT_DOMAIN_ICON_SIZE;
        const domainFontSize = Math.round((domainIconSize * DEFAULT_DOMAIN_FONT_SIZE) / DEFAULT_DOMAIN_ICON_SIZE);
        return (
            <DomainContainer $size={containerSize}>
                <DomainColoredIcon domain={entity as Domain} size={domainIconSize} fontSize={domainFontSize} />
            </DomainContainer>
        );
    }

    if (entity.type === EntityType.GlossaryTerm || entity.type === EntityType.GlossaryNode) {
        const glossaryIconSize = size ?? DEFAULT_DOMAIN_ICON_SIZE;
        const glossaryInnerIconSize = Math.round(
            (glossaryIconSize * DEFAULT_GLOSSARY_ICON_SIZE) / DEFAULT_DOMAIN_ICON_SIZE,
        );
        return (
            <GlossaryContainer $size={containerSize}>
                <GlossaryEntityIcon
                    entity={entity as GlossaryTerm | GlossaryNode}
                    size={glossaryIconSize}
                    iconSize={glossaryInnerIconSize}
                />
            </GlossaryContainer>
        );
    }

    if (!hasSiblings && (platforms?.length ?? 0) > 1) {
        return (
            <Container $size={containerSize}>
                {platforms?.map((platform) => (
                    <IconContainer key={platform.urn}>
                        <PlatformIcon platform={platform} size={size ?? DEFAULT_SIBLING_ICON_SIZE} />
                    </IconContainer>
                ))}
            </Container>
        );
    }

    return (
        <Container $size={containerSize}>
            {entitiesToShowIcons?.map((entityToShowIcon) => (
                <IconContainer key={entityToShowIcon.urn}>
                    <SingleEntityIcon entity={entityToShowIcon} size={iconSize} />
                </IconContainer>
            ))}
        </Container>
    );
}
