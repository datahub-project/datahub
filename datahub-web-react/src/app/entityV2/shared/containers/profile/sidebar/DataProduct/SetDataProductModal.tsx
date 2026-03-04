import { LoadingOutlined } from '@ant-design/icons';
import { Empty, Select, message } from 'antd';
import { debounce } from 'lodash';
import React, { useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityContext } from '@app/entity/shared/EntityContext';
import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import { handleBatchError } from '@app/entityV2/shared/utils';
import ContextPath from '@app/previewV2/ContextPath';
import { useIsMultipleDataProductsEnabled } from '@app/shared/hooks/useIsMultipleDataProductsEnabled';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal, Text } from '@src/alchemy-components';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import { useGetRecommendations } from '@src/app/shared/recommendation';
import { getModalDomContainer } from '@src/utils/focus';

import { useBatchAddToDataProductsMutation, useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { DataHubPageModuleType, DataProduct, Entity, EntityType } from '@types';

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin: 5px;
`;

interface Props {
    urns: string[];
    currentDataProducts: DataProduct[];
    onModalClose: () => void;
    titleOverride?: string;
    onOkOverride?: (result: string) => void;
    setDataProducts?: (dataProducts: DataProduct[]) => void;
    refetch?: () => void;
}

export default function SetDataProductModal({
    urns,
    currentDataProducts,
    onModalClose,
    titleOverride,
    onOkOverride,
    setDataProducts,
    refetch,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { reloadByKeyType } = useReloadableContext();
    const { refetch: refetchEntity } = useEntityContext();
    const isMultipleDataProductsEnabled = useIsMultipleDataProductsEnabled();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [batchAddToDataProductsMutation] = useBatchAddToDataProductsMutation();

    const [selectedDataProducts, setSelectedDataProducts] = useState<DataProduct[]>(currentDataProducts);
    const inputEl = useRef(null);

    const [getSearchResults, { data, loading: searchLoading }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { recommendedData: recommendedDataProducts, loading: recommendationsLoading } = useGetRecommendations([
        EntityType.DataProduct,
    ]);
    const [showRecommendations, setShowRecommendations] = useState(true);
    const loading = recommendationsLoading || searchLoading;

    const displayedDataProducts: Entity[] =
        !showRecommendations && data?.autoCompleteForMultiple?.suggestions
            ? data?.autoCompleteForMultiple?.suggestions?.flatMap((suggestion) => suggestion.entities)
            : recommendedDataProducts;

    const handleSearch = useMemo(() => {
        const fetch = (text: string) => {
            if (text.trim()) {
                getSearchResults({
                    variables: {
                        input: {
                            types: [EntityType.DataProduct],
                            query: text.trim(),
                            limit: 10,
                        },
                    },
                });
            }
            setShowRecommendations(!text.trim());
        };
        return debounce(fetch, 100);
    }, [getSearchResults]);

    const sendAnalytics = () => {
        const isBatchAction = urns.length > 1;

        if (isBatchAction) {
            analytics.event({
                type: EventType.BatchEntityActionEvent,
                actionType: EntityActionType.SetDataProduct,
                entityUrns: urns,
            });
        } else {
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.SetDataProduct,
                entityUrn: urns[0],
            });
        }
    };

    const handleMutationSuccess = (successMessage: string) => {
        message.success({ content: successMessage, duration: 3 });
        setDataProducts?.(selectedDataProducts);
        sendAnalytics();
        onModalClose();
        setSelectedDataProducts([]);
        setTimeout(() => {
            refetch?.();
            refetchEntity?.();
            reloadByKeyType([getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets)]);
        }, 3000);
    };

    const handleMutationError = (e: any, errorMessage: string) => {
        message.destroy();
        message.error(
            handleBatchError(urns, e, {
                content: `${errorMessage} \n ${e.message || ''}`,
                duration: 3,
            }),
        );
    };

    function onOk() {
        if (selectedDataProducts.length === 0) return;

        if (onOkOverride) {
            onOkOverride(selectedDataProducts[0]?.urn);
            return;
        }

        if (isMultipleDataProductsEnabled) {
            const dataProductUrns = selectedDataProducts.map((dp) => dp.urn);
            batchAddToDataProductsMutation({
                variables: {
                    input: {
                        resourceUrns: urns,
                        dataProductUrns,
                    },
                },
            })
                .then(() => handleMutationSuccess('Updated Data Products!'))
                .catch((e) => handleMutationError(e, 'Failed to add assets to Data Products:'));
        } else {
            batchSetDataProductMutation({
                variables: {
                    input: {
                        resourceUrns: urns,
                        dataProductUrn: selectedDataProducts[0].urn,
                    },
                },
            })
                .then(() => handleMutationSuccess('Updated Data Product!'))
                .catch((e) => handleMutationError(e, 'Failed to add assets to Data Product:'));
        }
    }

    function onSelectDataProduct(urn: string) {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const dataProduct = displayedDataProducts?.find((entity) => entity.urn === urn);
        if (dataProduct) {
            if (isMultipleDataProductsEnabled) {
                setSelectedDataProducts((prev) => [...prev, dataProduct as DataProduct]);
            } else {
                setSelectedDataProducts([dataProduct as DataProduct]);
            }
        }
    }

    function onDeselect(urn: string) {
        setSelectedDataProducts((prev) => prev.filter((dp) => dp.urn !== urn));
    }

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setDataProductButton',
    });

    const selectValue = selectedDataProducts.map((dp) => entityRegistry.getDisplayName(EntityType.DataProduct, dp));

    const loadingOption = {
        label: (
            <LoadingWrapper>
                <LoadingOutlined />
            </LoadingWrapper>
        ),
        value: 'loading',
    };

    const options = displayedDataProducts.map((result) => {
        return {
            label: (
                <>
                    <Text size="md">{entityRegistry.getDisplayName(EntityType.DataProduct, result)}</Text>
                    <ContextPath
                        entityType={EntityType.DataProduct}
                        displayedEntityType="Data product"
                        parentEntities={getParentEntities(result as DataProduct, EntityType.DataProduct)}
                        entityTitleWidth={200}
                        numVisible={3}
                    />
                </>
            ),
            value: result.urn,
        };
    });

    return (
        <Modal
            title={titleOverride || (isMultipleDataProductsEnabled ? 'Set Data Products' : 'Set Data Product')}
            open
            onCancel={onModalClose}
            getContainer={getModalDomContainer}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onModalClose,
                },
                {
                    text: 'Save',
                    variant: 'filled',
                    disabled: selectedDataProducts.length === 0,
                    onClick: onOk,
                    id: 'setDataProductButton',
                },
            ]}
        >
            <Select
                autoFocus
                showSearch
                defaultOpen
                filterOption={false}
                mode={isMultipleDataProductsEnabled ? 'multiple' : undefined}
                defaultActiveFirstOption={false}
                placeholder="Search for Data Products..."
                onSelect={(urn: string) => onSelectDataProduct(urn)}
                onDeselect={(urn: string) => onDeselect(urn)}
                onSearch={handleSearch}
                style={{ width: '100%' }}
                ref={inputEl}
                value={selectValue}
                options={loading ? [loadingOption] : options}
                notFoundContent={
                    !loading ? (
                        <Empty
                            description="No Data Products Found"
                            image={Empty.PRESENTED_IMAGE_SIMPLE}
                            style={{ color: ANTD_GRAY[7] }}
                        />
                    ) : null
                }
            />
        </Modal>
    );
}
