import { Database } from '@phosphor-icons/react';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
import { SectionContainer } from '@app/entityV2/shared/summary/HeaderComponents';
import { useEntityRegistry } from '@app/useEntityRegistry';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { DocumentRelatedAsset } from '@types';

const SectionHeader = styled.h4`
    font-size: 16px;
    font-weight: 600;
    margin: 0;
    color: ${colors.gray[1700]};
`;

const RelatedList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const RelatedItem = styled(Link)`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px;
    border-radius: 6px;
    background-color: ${colors.gray[0]};
    border: 1px solid ${colors.gray[100]};
    text-decoration: none;
    color: inherit;
    transition: all 0.2s ease;

    &:hover {
        background-color: ${colors.green[0]};
        border-color: ${colors.green[100]};
    }
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    color: ${colors.green[700]};
`;

const ItemContent = styled.div`
    display: flex;
    flex-direction: column;
`;

const ItemTitle = styled.div`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[1700]};
`;

const ItemType = styled.div`
    font-size: 12px;
    color: ${colors.gray[1800]};
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
            <RelatedList>
                {relatedAssets.map((relatedAsset) => {
                    const { asset } = relatedAsset;
                    const url = entityRegistry.getEntityUrl(asset.type, asset.urn);
                    const displayName = entityRegistry.getDisplayName(asset.type, asset);
                    const typeName = entityRegistry.getEntityName(asset.type);

                    return (
                        <RelatedItem key={asset.urn} to={url}>
                            <IconWrapper>
                                {entityRegistry.getIcon(asset.type, 16, IconStyleType.SVG) || <Database size={16} />}
                            </IconWrapper>
                            <ItemContent>
                                <ItemTitle>{displayName}</ItemTitle>
                                <ItemType>{typeName}</ItemType>
                            </ItemContent>
                        </RelatedItem>
                    );
                })}
            </RelatedList>
        </SectionContainer>
    );
};
