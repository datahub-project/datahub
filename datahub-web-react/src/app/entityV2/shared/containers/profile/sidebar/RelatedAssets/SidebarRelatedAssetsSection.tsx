import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    width: 100%;
`;

type RelatedAsset = {
    asset?: {
        urn?: string | null;
        type?: string | null;
    } | null;
};

// Renders the `documentInfo.relatedAssets` array for native Document profiles. The default
// DataProductSection reads the reverse `dataProduct.relationships` membership, which is
// semantically different from a Document referencing arbitrary assets.
export const SidebarRelatedAssetsSection = () => {
    const { entityData } = useEntityData();
    const relatedAssets: RelatedAsset[] =
        (entityData as unknown as { info?: { relatedAssets?: RelatedAsset[] | null } })?.info?.relatedAssets ?? [];

    return (
        <SidebarSection
            title="Related Assets"
            content={
                <Content>
                    {relatedAssets.length > 0 ? (
                        relatedAssets.map((relatedAsset) => {
                            const entity = relatedAsset?.asset;
                            if (!entity?.urn) return null;
                            return <EntityLink key={entity.urn} entity={entity as never} />;
                        })
                    ) : (
                        <EmptySectionText message="No related assets" />
                    )}
                </Content>
            }
        />
    );
};
