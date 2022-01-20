import React from 'react';
import { EntityType, Owner, SearchInsight, SubTypes } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    insights,
    subTypes,
    logoComponent,
    logoUrl,
    entityCount,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    insights?: Array<SearchInsight> | null;
    subTypes?: SubTypes | null;
    logoUrl?: string | null;
    logoComponent?: JSX.Element;
    entityCount?: number;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const typeName = (subTypes?.typeNames?.length && subTypes?.typeNames[0]) || 'Container';
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Container, urn)}
            name={name || ''}
            description={description || ''}
            type={typeName}
            owners={owners}
            insights={insights}
            logoUrl={logoUrl || undefined}
            logoComponent={logoComponent}
            typeIcon={entityRegistry.getIcon(EntityType.Container, 12, IconStyleType.ACCENT)}
            entityCount={entityCount}
        />
    );
};
