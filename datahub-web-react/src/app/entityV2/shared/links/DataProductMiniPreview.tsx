import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { DataProduct, EntityType } from '../../../../types.generated';
import EntityCount from '../containers/profile/header/EntityCount';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { ANTD_GRAY, ANTD_GRAY_V2, REDESIGN_COLORS } from '../../../entity/shared/constants';

const DomainInfoContainer = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const DataProductDescription = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${ANTD_GRAY[7]};
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const DataProductTitle = styled.div`
    font-size: 16px;
    font-weight: 400;
    color: ${ANTD_GRAY[9]};
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const DomainContents = styled.div`
    font-size: 12px;
    font-weight: 400;
    color: ${ANTD_GRAY[7]};
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
`;

const Card = styled(Link)`
    align-items: center;
    background-color: ${ANTD_GRAY[1]};
    border: 1.5px solid ${ANTD_GRAY_V2[5]};
    border-radius: 10px;
    display: flex;
    justify-content: start;
    min-height: 96px;
    min-width: 160px;
    padding: 8px 16px 8px 16px;

    :hover {
        border: 1.5px solid ${REDESIGN_COLORS.BLUE};
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
