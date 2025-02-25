import React, { useMemo } from 'react';
import styled from 'styled-components';
import { Image } from 'antd';
import { Entity } from '@src/types.generated';
import { IconStyleType } from '@src/app/entityV2/Entity';
import useEntityUtils from '@src/app/entityV2/shared/hooks/useEntityUtils';
import { EntityIconProps } from './types';

const ImageIcon = styled(Image)<{ $size: number }>`
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    object-fit: contain;
    background-color: transparent;
`;

const Container = styled.div``;

const ICON_SIZE = 20;
const SIBLING_ICON_SIZE = 16;

interface EntityIconRendererProps {
    entity: Entity;
    size: number;
}

function EntityIconRenderer({ entity, size }: EntityIconRendererProps) {
    const { entityRegistry, getGenericProperties, getPlatformLogoUrl, getPlatformName } = useEntityUtils();

    const properties = getGenericProperties(entity);
    const platformLogoUrl = getPlatformLogoUrl(entity, properties);
    const platformName = getPlatformName(entity, properties);

    return (
        (platformLogoUrl && (
            <ImageIcon preview={false} src={platformLogoUrl} alt={platformName || ''} $size={size} />
        )) ||
        entityRegistry.getIcon(entity.type, size, IconStyleType.ACCENT)
    );
}

export default function DefaultEntityIcon({ entity, siblings }: EntityIconProps) {
    const hasSiblings = useMemo(() => (siblings?.length ?? 0) > 0, [siblings?.length]);
    const entitiesToShowIcons = useMemo(() => (hasSiblings ? siblings : [entity]), [hasSiblings, siblings, entity]);
    const iconSize = useMemo(() => (hasSiblings ? SIBLING_ICON_SIZE : ICON_SIZE), [hasSiblings]);

    return (
        <Container>
            {entitiesToShowIcons?.map((entityToShowIcon) => (
                <EntityIconRenderer entity={entityToShowIcon} key={entityToShowIcon.urn} size={iconSize} />
            ))}
        </Container>
    );
}
