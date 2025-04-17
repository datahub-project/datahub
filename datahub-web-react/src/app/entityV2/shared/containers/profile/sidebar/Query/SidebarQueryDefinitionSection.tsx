import React from 'react';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { QueryEntity } from '@types';

const DefinitionLink = styled.a`
    align-items: center;
    display: flex;
    margin-bottom: 4px;
    overflow: hidden;
    width: 100%;
`;

const DefinitionIcon = styled(PlatformIcon)`
    margin-right: 0.5em;
`;

const DefinitionName = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

export default function SidebarQueryDefinitionSection() {
    const query = useBaseEntity<{ entity: QueryEntity | null }>()?.entity;
    const entityRegistry = useEntityRegistry();

    if (!query?.properties?.origin) {
        return null;
    }

    const { origin } = query.properties;
    const entity = entityRegistry.getGenericEntityProperties(origin.type, origin);

    return (
        <SidebarSection
            title="Definition"
            content={
                <>
                    <DefinitionLink href={entityRegistry.getEntityUrl(origin.type, origin.urn)}>
                        {entity?.platform && (
                            <DefinitionIcon platform={entity?.platform} size={16} entityType={origin.type} />
                        )}
                        <DefinitionName>{entityRegistry.getDisplayName(origin.type, entity)}</DefinitionName>
                    </DefinitionLink>
                </>
            }
        />
    );
}
