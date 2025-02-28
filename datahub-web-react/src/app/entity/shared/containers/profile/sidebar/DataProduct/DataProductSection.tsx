import { EditOutlined } from '@ant-design/icons';
import { Button, Modal, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { SidebarHeader } from '../SidebarHeader';
import { useEntityData } from '../../../../EntityContext';
import { EMPTY_MESSAGES } from '../../../../constants';
import SetDataProductModal from './SetDataProductModal';
import { DataProductLink } from '../../../../../../shared/tags/DataProductLink';
import { useBatchSetDataProductMutation } from '../../../../../../../graphql/dataProduct.generated';
import { DataProduct } from '../../../../../../../types.generated';

const EmptyText = styled(Typography.Paragraph)`
    &&& {
        border-top: none;
        padding-top: 0;
    }
`;

interface Props {
    readOnly?: boolean;
}

export default function DataProductSection({ readOnly }: Props) {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const { entityData, urn } = useEntityData();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [dataProduct, setDataProduct] = useState<DataProduct | null>(null);
    const dataProductRelationships = entityData?.dataProduct?.relationships;
    const siblingUrns: string[] =
        entityData?.siblingsSearch?.searchResults?.map((sibling) => sibling.entity.urn || '') || [];

    useEffect(() => {
        if (dataProductRelationships && dataProductRelationships.length > 0) {
            setDataProduct(dataProductRelationships[0].entity as DataProduct);
        }
    }, [dataProductRelationships]);

    function removeDataProduct() {
        batchSetDataProductMutation({ variables: { input: { resourceUrns: [urn, ...siblingUrns] } } })
            .then(() => {
                message.success({ content: 'Removed Data Product.', duration: 2 });
                setDataProduct(null);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: `Failed to remove data product. An unknown error occurred.`,
                        duration: 3,
                    });
                }
            });
    }

    const onRemoveDataProduct = () => {
        Modal.confirm({
            title: `Confirm Data Product Removal`,
            content: `Are you sure you want to remove this data product?`,
            onOk() {
                removeDataProduct();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <div>
            <SidebarHeader title="Data Product" />
            {dataProduct && (
                <DataProductLink
                    dataProduct={dataProduct}
                    closable={!readOnly}
                    readOnly={readOnly}
                    onClose={(e) => {
                        e.preventDefault();
                        onRemoveDataProduct();
                    }}
                    fontSize={12}
                />
            )}
            {!dataProduct && (
                <>
                    <EmptyText type="secondary">
                        {EMPTY_MESSAGES.dataProduct.title}. {EMPTY_MESSAGES.dataProduct.description}
                    </EmptyText>
                    {!readOnly && (
                        <Button type="default" onClick={() => setIsModalVisible(true)}>
                            <EditOutlined /> Set Data Product
                        </Button>
                    )}
                </>
            )}
            {isModalVisible && (
                <SetDataProductModal
                    urns={[urn, ...siblingUrns]}
                    currentDataProduct={dataProduct || null}
                    onModalClose={() => setIsModalVisible(false)}
                    setDataProduct={setDataProduct}
                />
            )}
        </div>
    );
}
