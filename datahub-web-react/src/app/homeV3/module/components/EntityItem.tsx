import React, { useCallback } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubPageModuleType, Entity } from '@types';

const StyledLink = styled(Link)`
    width: 100%;
`;

interface Props {
    entity: Entity;
    moduleType: DataHubPageModuleType;
    customDetailsRenderer?: (entity: Entity) => React.ReactNode;
    navigateOnlyOnNameClick?: boolean;
    dragIconRenderer?: () => React.ReactNode;
    hideSubtitle?: boolean;
    hideMatches?: boolean;
    padding?: string;
}

export default function EntityItem({
    entity,
    moduleType,
    customDetailsRenderer,
    navigateOnlyOnNameClick = false,
    dragIconRenderer,
    hideSubtitle,
    hideMatches,
    padding,
}: Props) {
    const entityRegistry = useEntityRegistryV2();

    // TODO: should we add when navigateOnlyOnNameClick is true?
    const sendAnalytics = useCallback(
        () =>
            analytics.event({
                type: EventType.HomePageTemplateModuleAssetClick,
                moduleType,
                assetUrn: entity.urn,
            }),
        [entity.urn, moduleType],
    );

    return (
        <>
            {navigateOnlyOnNameClick ? (
                <AutoCompleteEntityItem
                    entity={entity}
                    key={entity.urn}
                    customDetailsRenderer={customDetailsRenderer}
                    hideSubtitle={hideSubtitle}
                    hideMatches={hideMatches}
                    padding={padding}
                    navigateOnlyOnNameClick
                    dragIconRenderer={dragIconRenderer}
                />
            ) : (
                <StyledLink to={entityRegistry.getEntityUrl(entity.type, entity.urn)} onClick={sendAnalytics}>
                    <AutoCompleteEntityItem
                        entity={entity}
                        key={entity.urn}
                        hideSubtitle={hideSubtitle}
                        hideMatches={hideMatches}
                        padding={padding}
                        customDetailsRenderer={customDetailsRenderer}
                        dragIconRenderer={dragIconRenderer}
                    />
                </StyledLink>
            )}
        </>
    );
}
