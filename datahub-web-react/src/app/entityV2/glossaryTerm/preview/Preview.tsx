import React, { useMemo } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import GlossaryEntityIcon from '@app/glossaryV2/GlossaryEntityIcon';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Deprecation, Domain, EntityType, GlossaryTerm, Owner, ParentNodesResult } from '@types';

export const Preview = ({
    urn,
    name,
    data,
    description,
    owners,
    deprecation,
    parentNodes,
    previewType,
    domain,
    headerDropdownItems,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    deprecation?: Deprecation | null;
    parentNodes?: ParentNodesResult | null;
    previewType: PreviewType;
    domain?: Domain | undefined;
    headerDropdownItems?: Set<EntityMenuItems>;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const iconEntity = useMemo(
        () =>
            ({
                urn,
                type: EntityType.GlossaryTerm,
                displayProperties: data?.displayProperties ?? undefined,
                parentNodes: parentNodes ?? undefined,
            }) as Pick<GlossaryTerm, 'urn' | 'type' | 'displayProperties' | 'parentNodes'>,
        [urn, data?.displayProperties, parentNodes],
    );
    return (
        <DefaultPreviewCard
            previewType={previewType}
            url={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            owners={owners}
            entityIcon={<GlossaryEntityIcon entity={iconEntity} size={32} iconSize={18} />}
            entityType={EntityType.GlossaryTerm}
            typeIcon={entityRegistry.getIcon(EntityType.GlossaryTerm, 14, IconStyleType.ACCENT)}
            deprecation={deprecation}
            parentEntities={parentNodes?.nodes}
            domain={domain}
            headerDropdownItems={headerDropdownItems}
        />
    );
};
