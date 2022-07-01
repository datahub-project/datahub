import React from 'react';
import { EntityType, Owner, SearchInsight } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    count,
    insights,
    logoComponent,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    count?: number | null;
    insights?: Array<SearchInsight> | null;
    logoComponent?: JSX.Element;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Domain, urn)}
            name={name || ''}
            description={description || ''}
            type="Domain"
            owners={owners}
            insights={insights}
            logoComponent={logoComponent}
            entityCount={count || undefined}
        />
    );
};
