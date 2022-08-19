import { Tooltip } from 'antd';
import React from 'react';
import { useHistory } from 'react-router';
import { Entity, SearchResult } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityPreviewTag } from './EntityPreviewTag';

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
                        <Tooltip
                            visible={showTooltips ? undefined : false}
                            color="white"
                            placement="topRight"
                            overlayStyle={{ minWidth: 400 }}
                            overlayInnerStyle={{ padding: 12 }}
                            title={
                                <a href={url}>
                                    {entityRegistry.renderSearchResult(entity.type, {
                                        entity,
                                        matchedFields: [],
                                    } as SearchResult)}
                                </a>
                            }
                        >
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
                        </Tooltip>
                    </span>
                );
            })}
        </>
    );
};
