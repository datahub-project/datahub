import React from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import StructuredPropertyBadge from '@app/entityV2/shared/containers/profile/header/StructuredPropertyBadge';
import VersioningBadge from '@app/entityV2/shared/versioning/VersioningBadge';
import HealthIcon from '@app/previewV2/HealthIcon';

import { Entity } from '@types';

const Badges = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type Props = {
    entity: Entity;
    properties: GenericEntityProperties | null;
    entityUrl: string;
};

/**
 * The at-a-glance status the search-card header shows next to the name: deprecation, health,
 * the asset badge structured property, and the version tag. Each one renders only when the
 * entity carries that data, so most cards get no badge row at all.
 */
export default function HoverCardStatusBadges({ entity, properties, entityUrl }: Props) {
    const deprecation = properties?.deprecation;
    const health = properties?.health ?? [];
    const structuredProperties = properties?.structuredProperties;
    const versionProperties = properties?.versionProperties ?? undefined;

    const hasBadge =
        !!deprecation?.deprecated ||
        health.length > 0 ||
        !!structuredProperties?.properties?.length ||
        !!versionProperties?.version.versionTag;

    if (!hasBadge) return null;

    return (
        <Badges>
            {deprecation?.deprecated && (
                <DeprecationIcon urn={entity.urn} deprecation={deprecation} showUndeprecate={false} showText={false} />
            )}
            {health.length > 0 && <HealthIcon urn={entity.urn} health={health} baseUrl={entityUrl} />}
            <StructuredPropertyBadge
                structuredProperties={structuredProperties}
                platformUrn={properties?.platform?.urn}
            />
            <VersioningBadge versionProperties={versionProperties} showPopover={false} />
        </Badges>
    );
}
