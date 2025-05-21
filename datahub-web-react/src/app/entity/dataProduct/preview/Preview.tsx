import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain, EntityPath, EntityType, GlobalTags, GlossaryTerms, Owner } from '@types';

interface Props {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    globalTags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    entityCount?: number;
    externalUrl?: string | null;
    degree?: number;
    paths?: EntityPath[];
}

export const Preview = ({
    urn,
    name,
    description,
    owners,
    globalTags,
    domain,
    glossaryTerms,
    entityCount,
    externalUrl,
    degree,
    paths,
}: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataProduct, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            type={entityRegistry.getEntityName(EntityType.DataProduct)}
            typeIcon={entityRegistry.getIcon(EntityType.DataProduct, 12, IconStyleType.ACCENT)}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            glossaryTerms={glossaryTerms || undefined}
            entityCount={entityCount}
            externalUrl={externalUrl}
            displayAssetCount
            degree={degree}
            paths={paths}
        />
    );
};
