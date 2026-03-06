import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useEntityContext, useEntityData } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import SetDataProductModal from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/SetDataProductModal';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useIsMultipleDataProductsEnabled } from '@app/shared/hooks/useIsMultipleDataProductsEnabled';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { DataProductLink } from '@app/sharedV2/tags/DataProductLink';

import { useBatchRemoveFromDataProductsMutation, useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';
import { DataHubPageModuleType, DataProduct } from '@types';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
    text-wrap: wrap;
    gap: 4px 8px;
`;
interface Props {
    readOnly?: boolean;
}

export default function DataProductSection({ readOnly }: Props) {
    const { reloadByKeyType } = useReloadableContext();
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [showRemoveModal, setShowRemoveModal] = useState(false);
    const [dataProductToRemove, setDataProductToRemove] = useState<string | null>(null);
    const { entityData, urn } = useEntityData();
    const { refetch } = useEntityContext();
    const isMultipleDataProductsEnabled = useIsMultipleDataProductsEnabled();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [batchRemoveFromDataProductsMutation] = useBatchRemoveFromDataProductsMutation();
    const [dataProducts, setDataProducts] = useState<DataProduct[]>([]);
    const dataProductRelationships = entityData?.dataProduct?.relationships;
    const siblingUrns: string[] =
        entityData?.siblingsSearch?.searchResults?.map((sibling) => sibling.entity.urn || '') || [];

    const canEditDataProducts = !!entityData?.privileges?.canEditDataProducts;

    useEffect(() => {
        if (dataProductRelationships && dataProductRelationships.length > 0) {
            const allDataProducts = dataProductRelationships.map((rel) => rel.entity as DataProduct);
            setDataProducts(allDataProducts);
        } else {
            setDataProducts([]);
        }
    }, [dataProductRelationships]);

    function removeDataProduct() {
        if (!dataProductToRemove) return;

        if (isMultipleDataProductsEnabled) {
            batchRemoveFromDataProductsMutation({
                variables: {
                    input: {
                        resourceUrns: [urn, ...siblingUrns],
                        dataProductUrns: [dataProductToRemove],
                    },
                },
            })
                .then(() => {
                    message.success({ content: 'Removed from Data Product.', duration: 2 });
                    setDataProducts((prev) => prev.filter((dp) => dp.urn !== dataProductToRemove));
                    setShowRemoveModal(false);
                    setDataProductToRemove(null);
                    reloadByKeyType(
                        [
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.DataProducts),
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                        ],
                        3000,
                    );
                    setTimeout(refetch, 3000);
                })
                .catch((e: unknown) => {
                    message.destroy();
                    if (e instanceof Error) {
                        message.error({
                            content: `Failed to remove from data product. An unknown error occurred.`,
                            duration: 3,
                        });
                    }
                });
        } else {
            batchSetDataProductMutation({ variables: { input: { resourceUrns: [urn, ...siblingUrns] } } })
                .then(() => {
                    message.success({ content: 'Removed Data Product.', duration: 2 });
                    setDataProducts([]);
                    setShowRemoveModal(false);
                    setDataProductToRemove(null);
                    reloadByKeyType(
                        [
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.DataProducts),
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                        ],
                        3000,
                    );
                    setTimeout(refetch, 3000);
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
    }

    return (
        <>
            <SidebarSection
                title={isMultipleDataProductsEnabled ? 'Data Products' : 'Data Product'}
                content={
                    <Content>
                        {dataProducts.length > 0 ? (
                            dataProducts.map((dataProduct) => (
                                <DataProductLink
                                    key={dataProduct.urn}
                                    dataProduct={dataProduct}
                                    closable={!readOnly && isMultipleDataProductsEnabled}
                                    readOnly={readOnly}
                                    onClose={(e) => {
                                        e.preventDefault();
                                        setDataProductToRemove(dataProduct.urn);
                                        setShowRemoveModal(true);
                                    }}
                                    fontSize={12}
                                />
                            ))
                        ) : (
                            <EmptySectionText message={EMPTY_MESSAGES.dataProduct.title} />
                        )}
                    </Content>
                }
                extra={
                    <SectionActionButton
                        button={dataProducts.length > 0 ? <EditOutlinedIcon /> : <AddRoundedIcon />}
                        onClick={(event) => {
                            setIsModalVisible(true);
                            event.stopPropagation();
                        }}
                        actionPrivilege={canEditDataProducts}
                    />
                }
            />
            {isModalVisible && (
                <SetDataProductModal
                    urns={[urn, ...siblingUrns]}
                    currentDataProducts={dataProducts}
                    onModalClose={() => setIsModalVisible(false)}
                    setDataProducts={setDataProducts}
                />
            )}
            <ConfirmationModal
                isOpen={showRemoveModal}
                handleClose={() => {
                    setShowRemoveModal(false);
                    setDataProductToRemove(null);
                }}
                handleConfirm={removeDataProduct}
                modalTitle="Confirm Data Product Removal"
                modalText="Are you sure you want to remove this asset from the data product?"
            />
        </>
    );
}
