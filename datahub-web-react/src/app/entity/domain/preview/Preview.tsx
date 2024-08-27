import React from 'react';
import { useTranslation } from 'react-i18next';
import { Domain, EntityType, Owner, SearchInsight } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import DomainEntitiesSnippet from './DomainEntitiesSnippet';
import DomainIcon from '../../../domain/DomainIcon';

export const Preview = ({
    domain,
    urn,
    name,
    description,
    owners,
    insights,
    logoComponent,
}: {
    domain: Domain;
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    insights?: Array<SearchInsight> | null;
    logoComponent?: JSX.Element;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation();

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Domain, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            type={t('common.domain')}
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
        />
    );
};
