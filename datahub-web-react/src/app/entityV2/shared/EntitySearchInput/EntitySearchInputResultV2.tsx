import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import {
    getEntityDisplayName as getEntityDisplayNameUtil,
    getEntityTypeLabel,
} from '@app/entityV2/shared/EntitySearchSelect/utils';
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
    const { t } = useTranslation('entity.shared.selectors');
    const entityRegistry = useEntityRegistry() as EntityRegistry;
    const displayNameFromRegistry = entityRegistry.getDisplayName(entity.type, entity);

    // Fallback for entity types the registry has no entry for (INGESTION_SOURCE,
    // CUSTOM_OWNERSHIP_TYPE), which would otherwise render as a blank row. DefaultEntity's
    // displayName returns '' for those, so the falsy check alone covers them.
    if (!displayNameFromRegistry) {
        const displayName = getEntityDisplayNameUtil(entity, entityRegistry) || entity.urn || 'Unknown';

        return (
            <Wrapper>
                <TextWrapper>
                    <Text size="md" data-testid={`entity-${entity.urn}`}>
                        {displayName}
                    </Text>
                    <ContextPath
                        entityType={entity.type}
                        displayedEntityType={getEntityTypeLabel(entity.type, t)}
                        browsePaths={undefined}
                        parentEntities={undefined}
                        linksDisabled
                    />
                </TextWrapper>
            </Wrapper>
        );
    }

    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformIcon = properties?.platform?.properties?.logoUrl;

    const displayedEntityType = getDisplayedEntityType(properties, entityRegistry, entity.type);

    return (
        <Wrapper>
            {platformIcon && <IconContainer src={platformIcon} />}
            <TextWrapper>
                <Text size="md" data-testid={`entity-${entity.urn}`}>
                    {displayNameFromRegistry}
                </Text>
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
