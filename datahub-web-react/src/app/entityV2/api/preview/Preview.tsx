import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityMenuActions, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { getFirstSubType } from '@app/entityV2/shared/utils';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain, EntityPath, EntityType, GlobalTags, GlossaryTerms, Owner } from '@types';

interface Props {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    globalTags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    externalUrl?: string | null;
    degree?: number;
    paths?: EntityPath[];
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
    actions?: EntityMenuActions;
}

export const Preview = ({
    urn,
    data,
    name,
    description,
    owners,
    globalTags,
    domain,
    glossaryTerms,
    externalUrl,
    degree,
    paths,
    headerDropdownItems,
    previewType,
    actions,
}: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Api, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.Api}
            type={getFirstSubType(data ?? undefined)}
            typeIcon={entityRegistry.getIcon(EntityType.Api, 14, IconStyleType.ACCENT)}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            parentEntities={domain ? [domain] : []}
            glossaryTerms={glossaryTerms || undefined}
            externalUrl={externalUrl}
            degree={degree}
            paths={paths}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
            actions={actions}
        />
    );
};
