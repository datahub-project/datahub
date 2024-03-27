import { EditOutlined, CloseOutlined } from '@ant-design/icons';
import { Button, Modal, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { DataProduct, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';
import EditDataProductModal from './EditDataProductModal';
import { useDeleteDataProductMutation } from '../../../../graphql/dataProduct.generated';
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
    const entityRegistry = useEntityRegistry();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [deleteDataProductMutation] = useDeleteDataProductMutation();

    function deleteDataProduct() {
        deleteDataProductMutation({ variables: { urn: dataProduct.urn } })
            .then(() => {
                message.success('Deleted Data Product');
                setDeletedDataProductUrns((currentUrns) => [...currentUrns, dataProduct.urn]);
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to delete Data Product. An unexpected error occurred' });
            });
    }

    function onRemove() {
        Modal.confirm({
            title: `Delete ${entityRegistry.getDisplayName(EntityType.DataProduct, dataProduct)}`,
            content: `Are you sure you want to delete this Data Product?`,
            onOk() {
                deleteDataProduct();
            },
            onCancel() { },
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    }

    return (
        <>
            <ResultWrapper>
                <PreviewWrapper>
                    {entityRegistry.renderPreview(EntityType.DataProduct, PreviewType.SEARCH, dataProduct)}
                </PreviewWrapper>
                <ButtonsWrapper>
                    <StyledButton icon={<EditOutlined />} onClick={() => setIsEditModalVisible(true)} />
                    <TransparentButton size="small" onClick={onRemove}>
                        <CloseOutlined size={5} /> Remove Term
                    </TransparentButton>
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
