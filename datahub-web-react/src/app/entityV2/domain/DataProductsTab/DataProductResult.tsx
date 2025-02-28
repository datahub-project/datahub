import { useEntityContext } from '@src/app/entity/shared/EntityContext';
import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { DataProduct, EntityType } from '../../../../types.generated';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';
import EditDataProductModal from './EditDataProductModal';
import { REDESIGN_COLORS } from '../../shared/constants';
import useDeleteEntity from '../../shared/EntityDropdown/useDeleteEntity';

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

    const { onDeleteEntity } = useDeleteEntity(dataProduct.urn, dataProduct.type, dataProduct, deleteDataProduct);

    function onDeleteDataProduct() {
        onDeleteEntity();
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
