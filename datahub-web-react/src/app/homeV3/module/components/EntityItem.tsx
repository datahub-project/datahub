import React, { useCallback } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { useGetModalLinkProps } from '@app/sharedV2/modals/useGetModalLinkProps';
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
    // For custom click action on entity (either entire container or just name depending on navigateOnlyOnNameClick)
    customOnEntityClick?: (entity: Entity) => void;
    // For custom hover action on entity name
    customHoverEntityName?: (entity: Entity, children: React.ReactNode) => React.ReactNode;
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
    customOnEntityClick,
    customHoverEntityName,
}: Props) {
    const entityRegistry = useEntityRegistryV2();
    const linkProps = useGetModalLinkProps();
    const { templateType } = usePageTemplateContext();

    const sendAnalytics = useCallback(
        () =>
            analytics.event({
                type: EventType.HomePageTemplateModuleAssetClick,
                moduleType,
                assetUrn: entity.urn,
                location: templateType,
            }),
        [entity.urn, moduleType, templateType],
    );

    const autoCompleteItemProps = {
        entity,
        key: entity.urn,
        hideSubtitle,
        hideMatches,
        padding,
        customDetailsRenderer,
        dragIconRenderer,
        customHoverEntityName,
        navigateOnlyOnNameClick,
        customOnEntityClick,
    };

    if (customOnEntityClick && !navigateOnlyOnNameClick) {
        return (
            <div
                role="button"
                tabIndex={0}
                onClick={() => customOnEntityClick(entity)}
                onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        customOnEntityClick(entity);
                    }
                }}
            >
                <AutoCompleteEntityItem {...autoCompleteItemProps} onClick={sendAnalytics} />
            </div>
        );
    }

    if (navigateOnlyOnNameClick) {
        return <AutoCompleteEntityItem {...autoCompleteItemProps} onClick={sendAnalytics} />;
    }

    return (
        <StyledLink to={entityRegistry.getEntityUrl(entity.type, entity.urn)} onClick={sendAnalytics} {...linkProps}>
            <AutoCompleteEntityItem {...autoCompleteItemProps} />
        </StyledLink>
    );
}
