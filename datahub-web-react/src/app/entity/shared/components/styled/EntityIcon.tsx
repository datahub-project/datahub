import React from 'react';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { PlatformIcon } from '../../../../search/filters/utils';
import { Entity } from '../../../../../types.generated';
import { IconStyleType } from '../../../Entity';
import { ANTD_GRAY } from '../../constants';

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
