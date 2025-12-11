/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import DomainIcon from '@app/domain/DomainIcon';
import DomainEntitiesSnippet from '@app/entity/domain/preview/DomainEntitiesSnippet';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain, EntityType, Owner, SearchInsight } from '@types';

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
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Domain, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            type="Domain"
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
