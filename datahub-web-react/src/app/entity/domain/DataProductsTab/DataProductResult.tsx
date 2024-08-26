import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { Button, Dropdown, Modal, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { DataProduct, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';
import EditDataProductModal from './EditDataProductModal';
import { MenuIcon } from '../../shared/EntityDropdown/EntityDropdown';
import { useDeleteDataProductMutation } from '../../../../graphql/dataProduct.generated';

const ResultWrapper = styled.div`
    background-color: white;
    border-radius: 8px;
    max-width: 1200px;
    margin: 0 auto 8px auto;
    padding: 8px 16px;
    display: flex;
    width: 100%;
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
`;

const StyledMenuIcon = styled(MenuIcon)`
    margin-left: 8px;
    height: 18px;
    width: 18px;
`;

const PreviewWrapper = styled.div`
    max-width: 94%;
    flex: 1;
`;

const MenuItem = styled.div``;

interface Props {
    dataProduct: DataProduct;
    onUpdateDataProduct: (dataProduct: DataProduct) => void;
    setDeletedDataProductUrns: React.Dispatch<React.SetStateAction<string[]>>;
}

export default function DataProductResult({ dataProduct, onUpdateDataProduct, setDeletedDataProductUrns }: Props) {
    const { t } = useTranslation();
    const entityRegistry = useEntityRegistry();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [deleteDataProductMutation] = useDeleteDataProductMutation();

    function deleteDataProduct() {
        deleteDataProductMutation({ variables: { urn: dataProduct.urn } })
            .then(() => {
                message.success(t('common.deletedDataProduct'));
                setDeletedDataProductUrns((currentUrns) => [...currentUrns, dataProduct.urn]);
            })
            .catch(() => {
                message.destroy();
                message.error({ content: t('crud.error.failedToDeleteDataProduct') });
            });
    }

    function onRemove() {
        Modal.confirm({
            title: `${t('crud.delete')} ${entityRegistry.getDisplayName(EntityType.DataProduct, dataProduct)}`,
            content: t('entity.areSureDeleteThisDataProduct'),
            onOk() {
                deleteDataProduct();
            },
            onCancel() {},
            okText: t('common.type'),
            maskClosable: true,
            closable: true,
        });
    }

    const items = [
        {
            key: '0',
            label: (
                <MenuItem onClick={onRemove}>
                    <DeleteOutlined /> &nbsp;{t('common.delete')}
                </MenuItem>
            ),
        },
    ];

    return (
        <>
            <ResultWrapper>
                <PreviewWrapper>
                    {entityRegistry.renderPreview(EntityType.DataProduct, PreviewType.SEARCH, dataProduct)}
                </PreviewWrapper>
                <ButtonsWrapper>
                    <StyledButton icon={<EditOutlined />} onClick={() => setIsEditModalVisible(true)} />
                    <Dropdown menu={{ items }} trigger={['click']}>
                        <StyledMenuIcon />
                    </Dropdown>
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
