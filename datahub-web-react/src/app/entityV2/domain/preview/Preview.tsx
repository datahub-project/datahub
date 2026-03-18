import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import DomainEntitiesSnippet from '@app/entityV2/domain/preview/DomainEntitiesSnippet';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import EntityCount from '@app/entityV2/shared/containers/profile/header/EntityCount';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain, EntityType, Owner, SearchInsight } from '@types';

export const Preview = ({
    domain,
    urn,
    data,
    name,
    description,
    owners,
    insights,
    logoComponent,
    entityCount,
    headerDropdownItems,
    previewType,
}: {
    domain: Domain;
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    insights?: Array<SearchInsight> | null;
    logoComponent?: JSX.Element;
    entityCount?: number;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Domain, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.Domain}
            typeIcon={entityRegistry.getIcon(EntityType.Domain, 14, IconStyleType.ACCENT)}
            owners={owners}
            insights={insights}
            logoComponent={logoComponent}
            parentEntities={domain.parentDomains?.domains}
            snippet={<DomainEntitiesSnippet domain={domain} />}
            subHeader={<EntityCount displayAssetsText entityCount={entityCount} />}
            entityIcon={<DomainColoredIcon domain={domain as Domain} size={28} />}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
};
