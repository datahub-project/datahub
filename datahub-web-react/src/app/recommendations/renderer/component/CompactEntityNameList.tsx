import React from 'react';
import { Entity } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityPreviewTag } from './EntityPreviewTag';

type Props = {
    entities: Array<Entity>;
    onClick?: (index: number) => void;
    linkUrlParams?: Record<string, string | boolean>;
};
export const CompactEntityNameList = ({ entities, onClick, linkUrlParams }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <>
            {entities.map((entity, index) => {
                const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
                const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
                const displayName = entityRegistry.getDisplayName(entity.type, entity);
                const fallbackIcon = entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT);
                const url = entityRegistry.getEntityUrl(entity.type, entity.urn, linkUrlParams);
                return (
                    <EntityPreviewTag
                        displayName={displayName}
                        url={url}
                        platformLogoUrl={platformLogoUrl || undefined}
                        platformLogoUrls={genericProps?.siblingPlatforms?.map(
                            (platform) => platform.properties?.logoUrl,
                        )}
                        logoComponent={fallbackIcon}
                        onClick={() => onClick?.(index)}
                    />
                );
            })}
        </>
    );
};
