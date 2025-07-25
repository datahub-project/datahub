import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { getDisplayedEntityType } from '@app/entityV2/shared/containers/profile/header/utils';
import ContextPath from '@app/previewV2/ContextPath';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

const Wrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
`;

const TextWrapper = styled.div`
    display: flex;
    flex-direction: column;
    // TODO: Add this as a prop if needed
    max-width: 600px;
`;

const IconContainer = styled.img`
    height: 24px;
    min-width: 24px;
`;

type Props = {
    entity: Entity;
};

export default function EntitySearchInputResultV2({ entity }: Props) {
    const entityRegistry = useEntityRegistry() as EntityRegistry;
    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformIcon = entityRegistry.getPlatformProperties?.(entity.type, entity)?.properties?.logoUrl;

    const displayedEntityType = getDisplayedEntityType(properties, entityRegistry, entity.type);

    return (
        <Wrapper>
            {platformIcon && <IconContainer src={platformIcon} />}
            <TextWrapper>
                <Text size="md">{entityRegistry.getDisplayName(entity.type, entity)}</Text>
                <ContextPath
                    entityType={entity.type}
                    displayedEntityType={displayedEntityType}
                    browsePaths={properties?.browsePathV2}
                    parentEntities={
                        properties?.parentContainers?.containers ||
                        properties?.parentDomains?.domains ||
                        properties?.parentNodes?.nodes
                    }
                    linksDisabled
                />
            </TextWrapper>
        </Wrapper>
    );
}
