import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import { EntityType, Owner, GlobalTags, GlossaryTerms, Domain, EntityPath } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityMenuActions, IconStyleType, PreviewType } from '../../Entity';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

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
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType?: PreviewType;
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
    headerDropdownItems,
    previewType,
    actions,
}: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataProduct, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.DataProduct}
            typeIcon={entityRegistry.getIcon(EntityType.DataProduct, 12, IconStyleType.ACCENT)}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            parentEntities={domain ? [domain] : []}
            glossaryTerms={glossaryTerms || undefined}
            entityCount={entityCount}
            externalUrl={externalUrl}
            degree={degree}
            paths={paths}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
            actions={actions}
        />
    );
};
