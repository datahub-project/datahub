import React from 'react';
import { Domain, EntityType, Owner, SearchInsight } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import DomainEntitiesSnippet from './DomainEntitiesSnippet';
import DomainIcon from '../../../domain/DomainIcon';
import EntityCount from '../../shared/containers/profile/header/EntityCount';
import { DomainColoredIcon } from '../../shared/links/DomainColoredIcon';

export const Preview = ({
    domain,
    urn,
    name,
    description,
    owners,
    insights,
    logoComponent,
    entityCount,
}: {
    domain: Domain;
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    insights?: Array<SearchInsight> | null;
    logoComponent?: JSX.Element;
    entityCount?: number;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Domain, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            entityType={EntityType.Domain}
            typeIcon={
                <DomainIcon
                    style={{
                        fontSize: 14,
                        color: '#BFBFBF',
                    }}
                />
            }
            owners={owners}
            insights={insights}
            logoComponent={logoComponent}
            parentEntities={domain.parentDomains?.domains}
            snippet={<DomainEntitiesSnippet domain={domain} />}
            subHeader={<EntityCount displayAssetsText entityCount={entityCount} />}
            entityIcon={<DomainColoredIcon domain={domain as Domain} size={28} />}
        />
    );
};
