import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { DataProduct, EntityType } from '../../../../types.generated';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';
import EditDataProductModal from './EditDataProductModal';
import { REDESIGN_COLORS } from '../../shared/constants';

const TransparentButton = styled(Button)`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    font-size: 12px;
    box-shadow: none;
    border: none;
    padding: 0px 10px;
    position: absolute;
    top: 3px;
    right: 80px;
    display: none;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

const ResultWrapper = styled.div`
    background-color: white;
    border-radius: 8px;
    max-width: 1200px;
    margin: 0 auto 8px auto;
    padding: 8px 16px;
    display: flex;
    width: 100%;

    &:hover ${TransparentButton} {
        display: inline-block;
    }
`;

const StyledButton = styled(Button)`
    border: none;
    box-shadow: none;
    outline: none;
    height: 18px;
    width: 18px;
    padding: 0;

    svg {
        height: 14px;
        width: 14px;
    }
`;

const ButtonsWrapper = styled.div`
    margin-left: 16px;
    display: flex;
    position: relative;
`;

const PreviewWrapper = styled.div`
    max-width: 94%;
    flex: 1;
`;

interface Props {
    dataProduct: DataProduct;
    onUpdateDataProduct: (dataProduct: DataProduct) => void;
    setDeletedDataProductUrns: React.Dispatch<React.SetStateAction<string[]>>;
}

export default function DataProductResult({ dataProduct, onUpdateDataProduct, setDeletedDataProductUrns }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    function deleteDataProduct() {
        setDeletedDataProductUrns((currentUrns) => [...currentUrns, dataProduct.urn]);
    }

    const actions = {
        onDelete: deleteDataProduct,
    };

    return (
        <>
            <ResultWrapper>
                <PreviewWrapper>
                    {entityRegistry.renderPreview(EntityType.DataProduct, PreviewType.PREVIEW, dataProduct, actions)}
                </PreviewWrapper>
                <ButtonsWrapper>
                    <StyledButton icon={<EditOutlined />} onClick={() => setIsEditModalVisible(true)} />
                </ButtonsWrapper>
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
