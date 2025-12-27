import React from 'react';
import { useParams } from 'react-router-dom';

import { EntityType } from '@app/types.generated';
import { useEntityRegistry } from '@app/useEntityRegistry';

export const OrganizationProfile = () => {
    const { urn } = useParams<{ urn: string }>();
    const entityRegistry = useEntityRegistry();

    if (!urn) return <div>Invalid organization URN</div>;

    // Use the entity registry to render the profile
    // This properly delegates to OrganizationEntity.renderProfile
    return <>{entityRegistry.renderProfile(EntityType.Organization, urn)}</>;
};
