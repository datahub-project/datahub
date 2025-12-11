/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BookmarkSimple } from '@phosphor-icons/react';
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { getRelatedAssetsUrl } from '@app/entityV2/glossaryTerm/utils';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import UrlButton from '@app/entityV2/shared/UrlButton';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { AttributionDetails } from '@app/sharedV2/propagation/types';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { Deprecation, Domain, EntityType, Owner, ParentNodesResult } from '@types';

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
    propagationDetails,
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
    propagationDetails?: AttributionDetails;
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
                <UrlButton href={resolveRuntimePath(getRelatedAssetsUrl(entityRegistry, urn))}>
                    View Related Assets
                </UrlButton>
            }
            headerDropdownItems={headerDropdownItems}
            propagationDetails={propagationDetails}
        />
    );
};
