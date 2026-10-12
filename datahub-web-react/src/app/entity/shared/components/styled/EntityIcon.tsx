import React from 'react';
import { useTheme } from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { PlatformIcon } from '@app/search/filters/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

interface Props {
    entity: Entity;
    size?: number;
}

export default function EntityIcon({ entity, size = 14 }: Props) {
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const logoUrl = genericEntityProps?.platform?.properties?.logoUrl;
    const icon = logoUrl ? (
        <PlatformIcon src={logoUrl} size={size} />
    ) : (
        entityRegistry.getIcon(entity.type, size, IconStyleType.ACCENT, theme.colors.text)
    );

    return <>{icon}</>;
}
