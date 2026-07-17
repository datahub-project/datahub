import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EntityMenuActions, { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import PlatformHeaderIcons from '@app/entityV2/shared/containers/profile/header/PlatformContent/PlatformHeaderIcons';
import { getEntityPlatforms } from '@app/entityV2/shared/containers/profile/header/utils';
import { EntityBackButton } from '@app/entityV2/shared/containers/profile/sidebar/EntityBackButton';
import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import { useEntityRegistry } from '@app/useEntityRegistry';

const HEADER_DROPDOWN_ITEMS = new Set([EntityMenuItems.SHARE]);

const Row = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    padding: 18px;
    position: relative;
    overflow: hidden;
`;

const LeftColumn = styled.div`
    min-width: 0;
    display: flex;
    align-items: center;
    gap: 8px;
`;

const RightColumn = styled.div`
    flex-shrink: 0;
    display: flex;
    align-items: center;
    gap: 8px;
    padding-left: 8px;
`;

const Name = styled.h1`
    font-size: 20px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    margin: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const PathText = styled.span`
    font-size: 13px;
    color: ${(props) => props.theme.colors.textSecondary};
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const EntityDetails = styled.div`
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

export default function SemanticModelHeader() {
    const { urn, entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const { platform, platforms } = getEntityPlatforms(entityType, entityData);
    const name = entityRegistry.getDisplayName(entityType, entityData);
    const path = (entityData as any)?.path ?? '';

    const genericProperties = entityData ? entityRegistry.getGenericEntityProperties(entityType, entityData) : null;

    return (
        <Row data-testid="semantic-model-header">
            <LeftColumn>
                <EntityBackButton />
                <PlatformHeaderIcons platform={platform as any} platforms={platforms as any} />
                <EntityDetails>
                    <Name data-testid="semantic-model-header-name">{name}</Name>
                    {path && <PathText data-testid="semantic-model-header-path">{path}</PathText>}
                </EntityDetails>
            </LeftColumn>
            <RightColumn>
                {genericProperties && (
                    <ViewInPlatform
                        urn={urn}
                        data={genericProperties}
                        isEntityPageHeader
                        shouldFillAllAvailableSpace={false}
                    />
                )}
                <EntityMenuActions menuItems={HEADER_DROPDOWN_ITEMS} />
            </RightColumn>
        </Row>
    );
}
