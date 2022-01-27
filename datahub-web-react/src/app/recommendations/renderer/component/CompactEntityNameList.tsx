import React from 'react';
import { Entity } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityPreviewTag } from './EntityPreviewTag';

type Props = {
    entities: Array<Entity>;
    onClick?: (index: number) => void;
};
export const CompactEntityNameList = ({ entities, onClick }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <>
            {entities.map((entity, index) => {
                const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
                const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
                const displayName = entityRegistry.getDisplayName(entity.type, entity);
                const fallbackIcon = entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT);
                const url = entityRegistry.getEntityUrl(entity.type, entity.urn);
                return (
                    <EntityPreviewTag
                        displayName={displayName}
                        url={url}
                        platformLogoUrl={platformLogoUrl || undefined}
                        logoComponent={fallbackIcon}
                        onClick={() => onClick?.(index)}
                    />
                );
            })}
        </>
    );
};
