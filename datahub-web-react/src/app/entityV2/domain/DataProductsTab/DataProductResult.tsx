import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { PreviewType } from '@app/entityV2/Entity';
import EditDataProductModal from '@app/entityV2/domain/DataProductsTab/EditDataProductModal';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { useEntityContext } from '@src/app/entity/shared/EntityContext';

import { DataProduct, EntityType } from '@types';

const TransparentButton = styled(Button)`
    color: ${REDESIGN_COLORS.RED_ERROR};
    font-size: 12px;
    box-shadow: none;
    border: none;
    display: none;
    padding: unset;
    align-items: center;
    &&& span {
        font-size: 12px;
    }

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        color: ${REDESIGN_COLORS.RED_ERROR};
    }
`;

const ResultWrapper = styled.div`
    padding: 20px;
    display: flex;
    align-items: center;
    border: 1px solid #ebecf0;
    background: ${REDESIGN_COLORS.WHITE};
    border-radius: 10px;

    &:hover ${TransparentButton} {
        display: flex;
    }
`;

const PreviewWrapper = styled.div`
    position: relative;
    flex: 1;
    max-width: 100%;
`;

interface Props {
    dataProduct: DataProduct;
    onUpdateDataProduct: (dataProduct: DataProduct) => void;
    setDeletedDataProductUrns: React.Dispatch<React.SetStateAction<string[]>>;
}

export default function DataProductResult({ dataProduct, onUpdateDataProduct, setDeletedDataProductUrns }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const { refetch } = useEntityContext();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    function deleteDataProduct() {
        setDeletedDataProductUrns((currentUrns) => [...currentUrns, dataProduct.urn]);
    }

    function onDeleteDataProduct() {
        deleteDataProduct();
        setTimeout(() => refetch(), 3000);
    }

    return (
        <>
            <ResultWrapper>
                <PreviewWrapper>
                    {entityRegistry.renderPreview(EntityType.DataProduct, PreviewType.PREVIEW, dataProduct, {
                        onDelete: onDeleteDataProduct,
                        onEdit: () => setIsEditModalVisible(true),
                    })}
                </PreviewWrapper>
            </ResultWrapper>
            {isEditModalVisible && (
                <EditDataProductModal
                    dataProduct={dataProduct}
                    onClose={() => setIsEditModalVisible(false)}
                    onUpdateDataProduct={onUpdateDataProduct}
                />
            )}
        </>
    );
}
