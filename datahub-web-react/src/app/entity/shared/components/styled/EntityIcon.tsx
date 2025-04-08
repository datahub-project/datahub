import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { PlatformIcon } from '@app/search/filters/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

interface Props {
    entity: Entity;
    size?: number;
}

export default function EntityIcon({ entity, size = 14 }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const logoUrl = genericEntityProps?.platform?.properties?.logoUrl;
    const icon = logoUrl ? (
        <PlatformIcon src={logoUrl} size={size} />
    ) : (
        entityRegistry.getIcon(entity.type, size, IconStyleType.ACCENT, ANTD_GRAY[9])
    );

    return <>{icon}</>;
}
