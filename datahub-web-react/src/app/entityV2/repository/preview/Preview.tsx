import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityMenuActions, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { getFirstSubType } from '@app/entityV2/shared/utils';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { BrowsePathV2, DataPlatform, Domain, EntityPath, EntityType, GlobalTags, GlossaryTerms, Owner } from '@types';

interface Props {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    platform?: DataPlatform | null;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    globalTags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    externalUrl?: string | null;
    degree?: number;
    paths?: EntityPath[];
    browsePaths?: BrowsePathV2;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
    actions?: EntityMenuActions;
}

export const Preview = ({
    urn,
    data,
    name,
    description,
    platform,
    owners,
    globalTags,
    domain,
    glossaryTerms,
    externalUrl,
    degree,
    paths,
    browsePaths,
    headerDropdownItems,
    previewType,
    actions,
}: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Repository, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.Repository}
            type={getFirstSubType(data ?? undefined)}
            typeIcon={entityRegistry.getIcon(EntityType.Repository, 14, IconStyleType.ACCENT)}
            platform={platform?.properties?.displayName || platform?.name}
            logoUrl={platform?.properties?.logoUrl || undefined}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            glossaryTerms={glossaryTerms || undefined}
            externalUrl={externalUrl}
            degree={degree}
            paths={paths}
            browsePaths={browsePaths}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
            actions={actions}
        />
    );
};
