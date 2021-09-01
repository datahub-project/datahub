import React from 'react';
import { LegacyEntityProfile } from '../../../shared/LegacyEntityProfile';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import analytics, { EventType } from '../../../analytics';

export enum TabType {
    Features = 'Features',
    Sources = 'Sources',
    Ownership = 'Ownership',
}

/**
 * Responsible for display the MLFeature Page
 */
export const MLFeatureProfile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <LegacyEntityProfile
            titleLink={`/${entityRegistry.getPathName(EntityType.Mlfeature)}/${urn}`}
            title={urn}
            header={<></>}
            onTabChange={(tab: string) => {
                analytics.event({
                    type: EventType.EntitySectionViewEvent,
                    entityType: EntityType.Mlfeature,
                    entityUrn: urn,
                    section: tab,
                });
            }}
        />
    );
};
