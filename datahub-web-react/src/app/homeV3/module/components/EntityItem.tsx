import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity } from '@types';

const StyledLink = styled(Link)`
    width: 100%;
`;

interface Props {
    entity: Entity;
    customDetailsRenderer?: (entity: Entity) => React.ReactNode;
    navigateOnlyOnNameClick?: boolean;
    dragIconRenderer?: () => React.ReactNode;
    hideSubtitle?: boolean;
    hideMatches?: boolean;
    padding?: string;
}

export default function EntityItem({
    entity,
    customDetailsRenderer,
    navigateOnlyOnNameClick = false,
    dragIconRenderer,
    hideSubtitle,
    hideMatches,
    padding,
}: Props) {
    const entityRegistry = useEntityRegistryV2();

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
                <StyledLink to={entityRegistry.getEntityUrl(entity.type, entity.urn)}>
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
