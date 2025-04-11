import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import { BookmarkSimple } from '@phosphor-icons/react';
import { Deprecation, Domain, EntityType, Owner, ParentNodesResult } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import UrlButton from '../../shared/UrlButton';
import { getRelatedAssetsUrl } from '../utils';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

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
    return (
        <DefaultPreviewCard
            previewType={previewType}
            url={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            owners={owners}
            logoComponent={<BookmarkSimple style={{ fontSize: '20px' }} />}
            entityType={EntityType.GlossaryTerm}
            typeIcon={entityRegistry.getIcon(EntityType.GlossaryTerm, 14, IconStyleType.ACCENT)}
            deprecation={deprecation}
            parentEntities={parentNodes?.nodes}
            domain={domain}
            entityTitleSuffix={
                <UrlButton href={getRelatedAssetsUrl(entityRegistry, urn)}>View Related Assets</UrlButton>
            }
            headerDropdownItems={headerDropdownItems}
        />
    );
};
