import React from 'react';
import { useHistory } from 'react-router';
import { Entity } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityPreviewTag } from './EntityPreviewTag';
import { HoverEntityTooltip } from './HoverEntityTooltip';

type Props = {
    entities: Array<Entity>;
    onClick?: (index: number) => void;
    linkUrlParams?: Record<string, string | boolean>;
    showTooltips?: boolean;
};
export const CompactEntityNameList = ({ entities, onClick, linkUrlParams, showTooltips = true }: Props) => {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();

    return (
        <>
            {entities.map((entity, index) => {
                if (!entity) return <></>;

                const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
                const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
                const displayName = entityRegistry.getDisplayName(entity.type, entity);
                const fallbackIcon = entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT);
                const url = entityRegistry.getEntityUrl(entity.type, entity.urn, linkUrlParams);
                return (
                    <span
                        onClickCapture={(e) => {
                            // prevents the search links from taking over
                            e.preventDefault();
                            history.push(url);
                        }}
                    >
                        <HoverEntityTooltip entity={entity} canOpen={showTooltips}>
                            <span data-testid={`compact-entity-link-${entity.urn}`}>
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
                            </span>
                        </HoverEntityTooltip>
                    </span>
                );
            })}
        </>
    );
};
