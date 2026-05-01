import { toast } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRootEntityData } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import SetDataProductModal from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/SetDataProductModal';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import handleGraphQLError from '@app/shared/handleGraphQLError';
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
    const { reloadByKeyType, bypassCacheForUrn } = useReloadableContext();
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [showRemoveModal, setShowRemoveModal] = useState(false);
    const [dataProductToRemove, setDataProductToRemove] = useState<string | null>(null);
    const { entityData, urn } = useEntityData();
    const mutationUrn = useMutationUrn();
    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const rootEntityData = useRootEntityData();
    const isMultipleDataProductsEnabled = useIsMultipleDataProductsEnabled();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [batchRemoveFromDataProductsMutation] = useBatchRemoveFromDataProductsMutation();
    const [dataProducts, setDataProducts] = useState<DataProduct[]>([]);
    const dataProductRelationships = entityData?.dataProduct?.relationships;
    const canEditDataProducts = !!entityData?.privileges?.canEditDataProducts;

    // Use a stable string key instead of the array reference as the effect dependency.
    const dataProductUrnKey = (dataProductRelationships || [])
        .map((r) => r.entity?.urn)
        .sort()
        .join(',');

    useEffect(() => {
        if (dataProductRelationships && dataProductRelationships.length > 0) {
            const allDataProducts = dataProductRelationships.map((rel) => rel.entity as DataProduct);
            setDataProducts(allDataProducts);
        } else {
            setDataProducts([]);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [dataProductUrnKey]);

    // Returns the URNs (root and/or siblings) that a given data product is associated with,
    // so that remove mutations target only the entities that actually have the relationship.
    function getAssociatedUrns(dataProductUrn: string): string[] {
        if (isSeparateSiblingsMode) {
            return [urn];
        }

        const assocUrns: string[] = [];

        // Check the root entity using the non-combined data so we don't pick up
        // relationships that were contributed only by a sibling.
        const rootRelationships = rootEntityData?.dataProduct?.relationships || [];
        if (rootRelationships.some((r) => r.entity?.urn === dataProductUrn)) {
            assocUrns.push(urn);
        }

        // Check each sibling's individual (non-merged) data product relationships.
        entityData?.siblingsSearch?.searchResults?.forEach((result) => {
            const siblingRelationships: any[] = (result.entity as any)?.dataProduct?.relationships || [];
            if (siblingRelationships.some((r: any) => r.entity?.urn === dataProductUrn)) {
                assocUrns.push(result.entity.urn);
            }
        });

        return assocUrns.length > 0 ? assocUrns : [mutationUrn];
    }

    function removeDataProduct() {
        if (!dataProductToRemove) return;

        const associatedUrns = getAssociatedUrns(dataProductToRemove);

        if (isMultipleDataProductsEnabled) {
            batchRemoveFromDataProductsMutation({
                variables: {
                    input: {
                        resourceUrns: associatedUrns,
                        dataProductUrns: [dataProductToRemove],
                    },
                },
            })
                .then(() => {
                    toast.success('Removed from Data Product.', { duration: 2 });
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
                    associatedUrns.forEach((entityUrn) => bypassCacheForUrn(entityUrn));
                })
                .catch((error) =>
                    handleGraphQLError({
                        error,
                        defaultMessage: 'Failed to remove from data product. An unexpected error occurred',
                        permissionMessage:
                            'Unauthorized to remove from data product. Please contact your DataHub administrator.',
                    }),
                );
        } else {
            batchSetDataProductMutation({ variables: { input: { resourceUrns: associatedUrns } } })
                .then(() => {
                    toast.success('Removed Data Product.', { duration: 2 });
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
                    associatedUrns.forEach((entityUrn) => bypassCacheForUrn(entityUrn));
                })
                .catch((error) =>
                    handleGraphQLError({
                        error,
                        defaultMessage: 'Failed to remove from data product. An unexpected error occurred',
                        permissionMessage:
                            'Unauthorized to remove from data product. Please contact your DataHub administrator.',
                    }),
                );
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
                        icon={Plus}
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
                    urns={[mutationUrn]}
                    currentDataProducts={[]}
                    onModalClose={() => setIsModalVisible(false)}
                    setDataProducts={(newProducts) => {
                        if (isMultipleDataProductsEnabled) {
                            setDataProducts((prev) => [
                                ...prev,
                                ...newProducts.filter((np) => !prev.some((p) => p.urn === np.urn)),
                            ]);
                        } else {
                            setDataProducts(newProducts);
                        }
                    }}
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
