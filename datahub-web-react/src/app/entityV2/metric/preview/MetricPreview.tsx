import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Deprecation, Domain, EntityPath, EntityType, GlobalTags, GlossaryTerms, Owner } from '@types';

type Props = {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    globalTags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    degree?: number;
    paths?: EntityPath[];
    deprecation?: Deprecation | null;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
};

export default function MetricPreview({
    urn,
    data,
    name,
    description,
    owners,
    globalTags,
    domain,
    glossaryTerms,
    degree,
    paths,
    deprecation,
    headerDropdownItems,
    previewType,
}: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Metric, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.Metric}
            typeIcon={entityRegistry.getIcon(EntityType.Metric, 14, IconStyleType.ACCENT)}
            owners={owners}
            domain={domain}
            tags={globalTags || undefined}
            glossaryTerms={glossaryTerms || undefined}
            degree={degree}
            paths={paths}
            deprecation={deprecation}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
}
