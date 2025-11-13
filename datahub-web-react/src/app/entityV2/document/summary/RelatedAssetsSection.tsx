import React from 'react';
import styled from 'styled-components';

import { SectionContainer } from '@app/entityV2/shared/summary/HeaderComponents';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';
import { useEntityRegistry } from '@app/useEntityRegistry';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { DocumentRelatedAsset, EntityType } from '@types';

const SectionHeader = styled.h4`
    font-size: 16px;
    font-weight: 600;
    margin: 0 0 8px 0;
    color: ${colors.gray[1700]};
`;

const List = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

interface RelatedAssetsSectionProps {
    relatedAssets?: DocumentRelatedAsset[];
}

export const RelatedAssetsSection: React.FC<RelatedAssetsSectionProps> = ({ relatedAssets }) => {
    const entityRegistry = useEntityRegistry();

    if (!relatedAssets || relatedAssets.length === 0) {
        return null;
    }

    return (
        <SectionContainer>
            <SectionHeader>Related Assets</SectionHeader>
            <List>
                {relatedAssets.map((relatedAsset) => {
                    const { asset } = relatedAsset;
                    const genericProperties = entityRegistry.getGenericEntityProperties(
                        asset.type as EntityType,
                        asset,
                    );

                    return <EntityLink key={asset.urn} entity={genericProperties} showHealthIcon showDeprecatedIcon />;
                })}
            </List>
        </SectionContainer>
    );
};
