import React, { useEffect, useState } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityMenuActions, IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Deprecation, Domain, EntityPath, EntityType, GlobalTags, GlossaryTerms, Owner } from '@types';

interface Props {
    urn: string;
    data: GenericEntityProperties | null;
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
    deprecation?: Deprecation | null;
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
    entityCount,
    externalUrl,
    degree,
    paths,
    deprecation,
    headerDropdownItems,
    previewType,
    actions,
}: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    // Local copy so un-deprecating via the badge popup removes the badge immediately
    // without requiring a full search refetch (no DomainsContext equivalent for data products).
    const [localDeprecation, setLocalDeprecation] = useState(deprecation ?? null);
    useEffect(() => {
        setLocalDeprecation(deprecation ?? null);
    }, [deprecation]);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataProduct, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.DataProduct}
            typeIcon={entityRegistry.getIcon(EntityType.DataProduct, 14, IconStyleType.ACCENT)}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            parentEntities={data ? getParentEntities(data, EntityType.DataProduct) : []}
            glossaryTerms={glossaryTerms || undefined}
            entityCount={entityCount}
            externalUrl={externalUrl}
            degree={degree}
            paths={paths}
            deprecation={localDeprecation}
            refetchDeprecation={() => setLocalDeprecation(null)}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
            actions={actions}
        />
    );
};
