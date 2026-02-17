import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import EntityCount from '@app/entityV2/shared/containers/profile/header/EntityCount';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataProduct, EntityType } from '@types';

const DomainInfoContainer = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const DataProductDescription = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textTertiary};
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const DataProductTitle = styled.div`
    font-size: 16px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const DomainContents = styled.div`
    font-size: 12px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textTertiary};
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
`;

const Card = styled(Link)`
    align-items: center;
    background-color: ${(props) => props.theme.colors.bg};
    border: 1.5px solid ${(props) => props.theme.colors.border};
    border-radius: 10px;
    display: flex;
    justify-content: start;
    min-height: 96px;
    min-width: 160px;
    padding: 8px 16px 8px 16px;

    :hover {
        border: 1.5px solid ${(props) => props.theme.colors.textInformation};
        cursor: pointer;
    }
`;

export const DataProductMiniPreview = ({ dataProduct }: { dataProduct: DataProduct }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const url = entityRegistry.getEntityUrl(EntityType.DataProduct, dataProduct.urn as string);
    const assetCount = dataProduct?.entities?.total;
    return (
        <Card to={url}>
            <HoverEntityTooltip entity={dataProduct} placement="bottom" showArrow={false}>
                <DomainInfoContainer>
                    <DataProductTitle>{dataProduct?.properties?.name}</DataProductTitle>
                    <DataProductDescription>{dataProduct?.properties?.description}</DataProductDescription>
                    <DomainContents>
                        {assetCount ? <EntityCount displayAssetsText entityCount={assetCount} /> : null}
                    </DomainContents>
                </DomainInfoContainer>
            </HoverEntityTooltip>
        </Card>
    );
};
