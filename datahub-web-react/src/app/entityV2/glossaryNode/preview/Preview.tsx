import React, { useMemo } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import GlossaryEntityIcon from '@app/glossaryV2/GlossaryEntityIcon';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryNode, Owner, ParentNodesResult } from '@types';

export const Preview = ({
    urn,
    data,
    name,
    description,
    owners,
    parentNodes,
    headerDropdownItems,
    previewType,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    parentNodes?: ParentNodesResult | null;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const iconEntity = useMemo(
        () =>
            ({
                urn,
                type: EntityType.GlossaryNode,
                displayProperties: data?.displayProperties ?? undefined,
                parentNodes: parentNodes ?? undefined,
            }) as Pick<GlossaryNode, 'urn' | 'type' | 'displayProperties' | 'parentNodes'>,
        [urn, data?.displayProperties, parentNodes],
    );
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.GlossaryNode, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            owners={owners}
            entityIcon={<GlossaryEntityIcon entity={iconEntity} size={32} iconSize={18} />}
            entityType={EntityType.GlossaryNode}
            parentEntities={parentNodes?.nodes}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
};
