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
    margin: 0;
    color: ${colors.gray[600]};
`;

const List = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const EmptyState = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[600]};
    text-align: center;
    padding: 8px;
`;

interface RelatedAssetsSectionProps {
    relatedAssets?: DocumentRelatedAsset[];
}

export const RelatedAssetsSection: React.FC<RelatedAssetsSectionProps> = ({ relatedAssets }) => {
    const entityRegistry = useEntityRegistry();

    return (
        <SectionContainer>
            <SectionHeader>Related</SectionHeader>
            <List>
                {relatedAssets?.map((relatedAsset) => {
                    const { asset } = relatedAsset;
                    const genericProperties = entityRegistry.getGenericEntityProperties(
                        asset.type as EntityType,
                        asset,
                    );

                    return <EntityLink key={asset.urn} entity={genericProperties} showHealthIcon showDeprecatedIcon />;
                })}
                {relatedAssets?.length === 0 || !relatedAssets ? <EmptyState>No related assets yet</EmptyState> : null}
            </List>
        </SectionContainer>
    );
};
